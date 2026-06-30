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
    /// Balance mapping base slot per token (the `balanceOf` mapping's slot).
    balance_slots: HashMap<Address, U256>,
    /// Fallback balance mapping base slot for tokens not in the map.
    default_balance_slot: U256,
}

impl Erc20TransferDecoder {
    /// Create a decoder with `default_balance_slot` as the `balanceOf` mapping
    /// base slot for any token without a per-token override.
    pub fn new(default_balance_slot: U256) -> Self {
        Self {
            balance_slots: HashMap::new(),
            default_balance_slot,
        }
    }

    /// Override the `balanceOf` mapping base slot for `token` (builder style).
    pub fn with_token(mut self, token: Address, balance_slot: U256) -> Self {
        self.balance_slots.insert(token, balance_slot);
        self
    }

    /// The configured balance mapping base slot for `token` (its override, else
    /// the default).
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

        let slot = self.balance_slot(transfer.token);
        let mut updates = Vec::with_capacity(2);

        // Skip the zero-address leg (mint = from == 0, burn = to == 0).
        if transfer.from != Address::ZERO {
            updates.push(StateUpdate::slot_delta(
                transfer.token,
                balance_key(transfer.from, slot),
                SlotDelta::Sub(transfer.value),
            ));
        }
        if transfer.to != Address::ZERO {
            updates.push(StateUpdate::slot_delta(
                transfer.token,
                balance_key(transfer.to, slot),
                SlotDelta::Add(transfer.value),
            ));
        }
        updates
    }
}
