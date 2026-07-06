//! Generic ERC-20 `Transfer` decoder (generic core).
//!
//! [`Erc20TransferDecoder`] turns a standard ERC-20
//! `Transfer(from, to, value)` log into two relative balance updates — a
//! [`SlotDelta::Sub`] on the sender's balance slot and a
//! [`SlotDelta::Add`] on the recipient's — so the cache
//! tracks balances from the event stream without ever reading the resulting
//! absolute balances. It is the log-driven form of the Phase 3 reactive-balance
//! case.
//!
//! # Balance-slot derivation
//!
//! An ERC-20 `balanceOf` is a `mapping(address => uint256)` at some base slot.
//! The decoder hashes the owner into that mapping the canonical Solidity way:
//! `keccak256(abi.encode(owner, balance_slot))`. The base slot is configurable
//! per token ([`with_token`](Erc20TransferDecoder::with_token)) with a default
//! fallback ([`new`](Erc20TransferDecoder::new)), since different tokens place
//! `balanceOf` at different slots.
//!
//! # Mint / burn legs
//!
//! A mint (`from == 0`) or burn (`to == 0`) has no real holder on the
//! zero-address leg, so that leg is **skipped** — only the non-zero side emits a
//! delta. Cold balances follow the Phase 3 contract: the
//! [`SlotDelta`] is skipped at apply time and surfaced in
//! [`StateDiff::skipped`](crate::StateDiff::skipped) (the caller seeds the
//! balance, or the next read lazily fetches it). The decoder ignores the
//! [`StateView`] — it is stateless.

use std::collections::HashMap;

use alloy_primitives::{Address, Log, U256, keccak256};
use alloy_sol_types::SolValue;

use crate::events::{EventDecoder, StateView};
use crate::inspector::TransferInspector;
use crate::mapping_probe::TrackedMapping;
use crate::state_update::{SlotDelta, StateUpdate};

/// Decodes ERC-20 `Transfer` logs into relative balance [`SlotDelta`] updates.
///
/// ```
/// use alloy_primitives::{Address, Bytes, Log, U256, keccak256};
/// use alloy_sol_types::SolValue;
/// use evm_fork_cache::events::{EventDecoder, StateView};
/// use evm_fork_cache::events::erc20::Erc20TransferDecoder;
/// use evm_fork_cache::{SlotDelta, StateUpdate};
///
/// // A read-only view that reports every slot cold (decoder is stateless anyway).
/// struct ColdView;
/// impl StateView for ColdView {
///     fn storage(&self, _: Address, _: U256) -> Option<U256> { None }
/// }
///
/// let token = Address::repeat_byte(0x20);
/// let from = Address::repeat_byte(0x21);
/// let to = Address::repeat_byte(0x22);
///
/// // Transfer(from, to, 100) log: balanceOf mapping at slot 3.
/// let sig = keccak256(b"Transfer(address,address,uint256)");
/// let log = Log::new_unchecked(
///     token,
///     vec![sig, from.into_word(), to.into_word()],
///     Bytes::copy_from_slice(&U256::from(100).to_be_bytes::<32>()),
/// );
///
/// let decoder = Erc20TransferDecoder::new(U256::from(3));
/// let updates = decoder.decode(&log, &ColdView);
///
/// let slot = |owner: Address| {
///     U256::from_be_bytes(keccak256((owner, U256::from(3)).abi_encode()).0)
/// };
/// assert_eq!(updates, vec![
///     StateUpdate::slot_delta(token, slot(from), SlotDelta::Sub(U256::from(100))),
///     StateUpdate::slot_delta(token, slot(to), SlotDelta::Add(U256::from(100))),
/// ]);
/// ```
pub struct Erc20TransferDecoder {
    /// Balance mapping base slot per token (the `balanceOf` mapping's slot),
    /// assuming Solidity `keccak(key‖slot)` layout.
    balance_slots: HashMap<Address, U256>,
    /// Layout-aware descriptors per token, taking precedence over
    /// `balance_slots` — set via [`with_tracked`](Self::with_tracked) for Vyper
    /// or Solady tokens whose byte order differs from Solidity's.
    tracked: HashMap<Address, TrackedMapping>,
    /// Fallback balance mapping base slot for tokens not in either map.
    default_balance_slot: U256,
}

impl Erc20TransferDecoder {
    /// Create a decoder with `default_balance_slot` as the `balanceOf` mapping
    /// base slot for any token without a per-token override.
    pub fn new(default_balance_slot: U256) -> Self {
        Self {
            balance_slots: HashMap::new(),
            tracked: HashMap::new(),
            default_balance_slot,
        }
    }

    /// Override the `balanceOf` mapping base slot for `token`, assuming
    /// Solidity's `keccak(key‖slot)` layout (builder style).
    pub fn with_token(mut self, token: Address, balance_slot: U256) -> Self {
        self.balance_slots.insert(token, balance_slot);
        self
    }

    /// Configure a token from a discovered [`TrackedMapping`], honoring its
    /// layout (Solidity / Vyper / Solady). Takes precedence over
    /// [`with_token`](Self::with_token) for the same token.
    ///
    /// Pair with
    /// [`EvmCache::discover_erc20_balance_slot`](crate::cache::EvmCache::discover_erc20_balance_slot):
    /// discover once, then feed the layout here so event-driven balance tracking
    /// writes the correct slot even for non-Solidity tokens.
    pub fn with_tracked(mut self, tracked: TrackedMapping) -> Self {
        self.tracked.insert(tracked.contract, tracked);
        self
    }

    /// The hashed storage slot of `owner`'s balance for `token`, honoring a
    /// tracked layout if present, else Solidity order at the configured base slot.
    fn entry_slot(&self, token: Address, owner: Address) -> U256 {
        if let Some(tracked) = self.tracked.get(&token)
            && let Some(slot) = tracked.slot_for(owner.into_word())
        {
            return U256::from_be_bytes(slot.0);
        }
        balance_key(owner, self.balance_slot(token))
    }

    /// The configured Solidity balance mapping base slot for `token` (its
    /// override, else the default).
    fn balance_slot(&self, token: Address) -> U256 {
        self.balance_slots
            .get(&token)
            .copied()
            .unwrap_or(self.default_balance_slot)
    }
}

/// The hashed storage slot of `balanceOf[owner]` for a `mapping(address =>
/// uint256)` at `mapping_slot`.
fn balance_key(owner: Address, mapping_slot: U256) -> U256 {
    U256::from_be_bytes(keccak256((owner, mapping_slot).abi_encode()).0)
}

impl EventDecoder for Erc20TransferDecoder {
    fn decode(&self, log: &Log, _view: &dyn StateView) -> Vec<StateUpdate> {
        // Reuse the canonical ERC-20 Transfer signature match + topic/data decode.
        // Returns None for a non-Transfer log (wrong topic0, <3 topics, <32 data
        // bytes).
        let Some(transfer) = TransferInspector::parse_transfer(log) else {
            return Vec::new();
        };

        let mut updates = Vec::with_capacity(2);

        // Skip the zero-address leg (mint = from == 0, burn = to == 0).
        if transfer.from != Address::ZERO {
            updates.push(StateUpdate::slot_delta(
                transfer.token,
                self.entry_slot(transfer.token, transfer.from),
                SlotDelta::Sub(transfer.value),
            ));
        }
        if transfer.to != Address::ZERO {
            updates.push(StateUpdate::slot_delta(
                transfer.token,
                self.entry_slot(transfer.token, transfer.to),
                SlotDelta::Add(transfer.value),
            ));
        }
        updates
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mapping_probe::{SlotLayout, TrackedMapping};
    use alloy_primitives::{B256, Log, address};

    /// The decoder ignores state, so a no-op `StateView` suffices.
    struct NoState;
    impl StateView for NoState {
        fn storage(&self, _address: Address, _slot: U256) -> Option<U256> {
            None
        }
    }

    fn transfer_log(token: Address, from: Address, to: Address, value: U256) -> Log {
        let sig = keccak256("Transfer(address,address,uint256)");
        Log::new(
            token,
            vec![sig, from.into_word(), to.into_word()],
            value.to_be_bytes_vec().into(),
        )
        .unwrap()
    }

    const TOKEN: Address = address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
    const HOLDER: Address = address!("00000000000000000000000000000000000000A1");

    /// A mint (`from == 0`) must emit ONLY the recipient leg — never a write to
    /// the zero address's balance slot.
    #[test]
    fn mint_skips_zero_address_from_leg() {
        let decoder = Erc20TransferDecoder::new(U256::from(3u64));
        let log = transfer_log(TOKEN, Address::ZERO, HOLDER, U256::from(100u64));
        let updates = decoder.decode(&log, &NoState);
        assert_eq!(
            updates,
            vec![StateUpdate::slot_delta(
                TOKEN,
                balance_key(HOLDER, U256::from(3u64)),
                SlotDelta::Add(U256::from(100u64)),
            )],
            "mint emits only the recipient leg; no zero-address write"
        );
    }

    /// A burn (`to == 0`) must emit ONLY the sender leg.
    #[test]
    fn burn_skips_zero_address_to_leg() {
        let decoder = Erc20TransferDecoder::new(U256::from(3u64));
        let log = transfer_log(TOKEN, HOLDER, Address::ZERO, U256::from(40u64));
        let updates = decoder.decode(&log, &NoState);
        assert_eq!(
            updates,
            vec![StateUpdate::slot_delta(
                TOKEN,
                balance_key(HOLDER, U256::from(3u64)),
                SlotDelta::Sub(U256::from(40u64)),
            )],
            "burn emits only the sender leg; no zero-address write"
        );
    }

    /// A zero-value self-transfer to/from the zero address (degenerate) emits
    /// nothing — belt-and-suspenders that neither leg touches slot 0's holder.
    #[test]
    fn zero_to_zero_emits_nothing() {
        let decoder = Erc20TransferDecoder::new(U256::from(3u64));
        let log = transfer_log(TOKEN, Address::ZERO, Address::ZERO, U256::from(5u64));
        assert!(decoder.decode(&log, &NoState).is_empty());
    }

    /// With a discovered non-Solidity layout, the emitted slot uses that byte
    /// order — and the zero leg is still skipped.
    #[test]
    fn layout_aware_uses_tracked_order_and_still_skips_zero() {
        let tracked = TrackedMapping::new(TOKEN, U256::from(2u64), SlotLayout::VyperMapping);
        let decoder = Erc20TransferDecoder::new(U256::from(3u64)).with_tracked(tracked);
        let log = transfer_log(TOKEN, Address::ZERO, HOLDER, U256::from(7u64));
        let updates = decoder.decode(&log, &NoState);

        // Vyper order: keccak(slot ‖ key), NOT the Solidity default slot 3.
        let expected_slot = {
            let mut pre = [0u8; 64];
            pre[31] = 2;
            pre[32..64].copy_from_slice(HOLDER.into_word().as_slice());
            B256::from(keccak256(pre))
        };
        assert_eq!(
            updates,
            vec![StateUpdate::slot_delta(
                TOKEN,
                U256::from_be_bytes(expected_slot.0),
                SlotDelta::Add(U256::from(7u64)),
            )]
        );
    }
}
