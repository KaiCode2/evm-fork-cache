//! ERC20 `Transfer`-event capture for reconstructing balance deltas.
//!
//! This module provides a [`revm::Inspector`] that watches logs emitted during
//! a simulation, matches the canonical ERC20 `Transfer(address,address,uint256)`
//! signature, and records each transfer. The captured transfers let callers
//! compute net balance changes per token and account without re-reading storage
//! after the call.
//!
//! # Parsing assumptions
//!
//! Transfers are decoded assuming the standard ERC20 event layout:
//! `from` and `to` come from the indexed topics (via [`Address::from_word`], i.e.
//! the low 20 bytes of each 32-byte topic) and `value` is read from the first 32
//! data bytes. A non-standard or packed `Transfer` event (e.g. one that does not
//! index `from`/`to`, or packs additional fields into the data) may parse
//! incorrectly or be silently skipped.
//!
//! Balance deltas are computed symmetrically: a self-transfer where `from == to`
//! is both subtracted and added, netting to zero for that owner.

use std::collections::HashMap;

use alloy_primitives::{Address, B256, I256, Log, U256};
use revm::Inspector;
use revm::interpreter::InterpreterTypes;

/// ERC20 `Transfer` event signature: `keccak256("Transfer(address,address,uint256)")`.
///
/// A log's first topic must equal this value to be treated as a transfer.
const TRANSFER_EVENT_SIGNATURE: B256 = B256::new([
    0xdd, 0xf2, 0x52, 0xad, 0x1b, 0xe2, 0xc8, 0x9b, 0x69, 0xc2, 0xb0, 0x68, 0xfc, 0x37, 0x8d, 0xaa,
    0x95, 0x2b, 0xa7, 0xf1, 0x63, 0xc4, 0xa1, 0x16, 0x28, 0xf5, 0x5a, 0x4d, 0xf5, 0x23, 0xb3, 0xef,
]);

/// A single ERC20 token transfer decoded from a `Transfer` log.
///
/// Fields are populated from the standard ERC20 event layout (see the
/// [module docs](crate::inspector) for caveats on non-standard events).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TokenTransfer {
    /// Address of the token contract that emitted the event (the log's address).
    pub token: Address,
    /// Sender, decoded from the first indexed topic.
    pub from: Address,
    /// Recipient, decoded from the second indexed topic.
    pub to: Address,
    /// Amount transferred, decoded from the first 32 data bytes.
    pub value: U256,
}

/// Inspector that captures ERC20 `Transfer` events during EVM execution.
///
/// Attach to a simulation and the [`Inspector::log`] hook records every emitted
/// log; logs matching the ERC20 `Transfer` layout are additionally decoded into
/// [`TokenTransfer`]s. Reconstruct net balance changes afterward with
/// [`balance_deltas`](Self::balance_deltas) or
/// [`balance_deltas_for_tokens`](Self::balance_deltas_for_tokens), and reuse the
/// inspector across calls via [`clear`](Self::clear).
#[derive(Clone, Debug, Default)]
pub struct TransferInspector {
    /// Token transfers decoded from captured logs.
    pub transfers: Vec<TokenTransfer>,
    /// Every log emitted during execution, retained for debugging/analysis.
    pub logs: Vec<Log>,
}

impl TransferInspector {
    /// Create an empty inspector with no captured transfers or logs.
    ///
    /// ```
    /// use evm_fork_cache::inspector::TransferInspector;
    ///
    /// let inspector = TransferInspector::new();
    /// assert!(inspector.transfers.is_empty());
    /// assert!(inspector.logs.is_empty());
    /// ```
    pub fn new() -> Self {
        Self {
            transfers: Vec::new(),
            logs: Vec::new(),
        }
    }

    /// Compute balance deltas for a specific owner from captured transfers
    ///
    /// Returns a map of token address -> signed balance change
    /// Positive values indicate tokens received, negative indicates tokens sent
    pub fn balance_deltas(&self, owner: Address) -> HashMap<Address, I256> {
        let mut deltas: HashMap<Address, I256> = HashMap::new();

        for transfer in &self.transfers {
            if transfer.from == owner {
                // Outgoing transfer - subtract from balance
                let entry = deltas.entry(transfer.token).or_insert(I256::ZERO);
                *entry -= I256::from_raw(transfer.value);
            }
            if transfer.to == owner {
                // Incoming transfer - add to balance
                let entry = deltas.entry(transfer.token).or_insert(I256::ZERO);
                *entry += I256::from_raw(transfer.value);
            }
        }

        deltas
    }

    /// Like [`balance_deltas`](Self::balance_deltas), but restricted to the
    /// given set of token addresses.
    ///
    /// Tokens in `tokens` with no transfers touching `owner` are simply absent
    /// from the result; tokens not in `tokens` are excluded even if `owner`
    /// transacted in them.
    ///
    /// ```
    /// # use evm_fork_cache::inspector::{TransferInspector, TokenTransfer};
    /// # use alloy_primitives::{Address, I256, U256};
    /// let mut inspector = TransferInspector::new();
    /// let token_a = Address::repeat_byte(0xAA);
    /// let token_b = Address::repeat_byte(0xBB);
    /// let owner = Address::repeat_byte(0x11);
    /// let other = Address::repeat_byte(0x22);
    /// inspector.transfers.push(TokenTransfer { token: token_a, from: owner, to: other, value: U256::from(100u64) });
    /// inspector.transfers.push(TokenTransfer { token: token_b, from: other, to: owner, value: U256::from(50u64) });
    ///
    /// let deltas = inspector.balance_deltas_for_tokens(owner, [token_a]);
    /// assert_eq!(deltas.len(), 1);
    /// assert_eq!(deltas.get(&token_a), Some(&(-I256::from_raw(U256::from(100u64)))));
    /// assert!(!deltas.contains_key(&token_b));
    /// ```
    pub fn balance_deltas_for_tokens(
        &self,
        owner: Address,
        tokens: impl IntoIterator<Item = Address>,
    ) -> HashMap<Address, I256> {
        let token_set: std::collections::HashSet<Address> = tokens.into_iter().collect();
        let all_deltas = self.balance_deltas(owner);

        all_deltas
            .into_iter()
            .filter(|(token, _)| token_set.contains(token))
            .collect()
    }

    /// Drop all captured transfers and logs so the inspector can be reused
    /// across simulations.
    pub fn clear(&mut self) {
        self.transfers.clear();
        self.logs.clear();
    }

    /// Parse a Transfer event from log topics and data
    pub fn parse_transfer(log: &Log) -> Option<TokenTransfer> {
        // ERC20 Transfer event has:
        // - topic[0]: event signature
        // - topic[1]: from address (indexed, 32 bytes padded)
        // - topic[2]: to address (indexed, 32 bytes padded)
        // - data: value (uint256)

        let topics = log.topics();
        if topics.len() < 3 {
            return None;
        }

        // Check event signature
        if topics[0] != TRANSFER_EVENT_SIGNATURE {
            return None;
        }

        // Extract addresses from topics (last 20 bytes of 32-byte topic)
        let from = Address::from_word(topics[1]);
        let to = Address::from_word(topics[2]);

        // Extract value from data (should be exactly 32 bytes for standard ERC20)
        let data = log.data.data.as_ref();
        if data.len() < 32 {
            return None;
        }
        let value = U256::from_be_slice(&data[..32]);

        Some(TokenTransfer {
            token: log.address,
            from,
            to,
            value,
        })
    }
}

/// Captures every emitted log via the [`Inspector::log`] hook.
///
/// Each log is pushed to [`logs`](TransferInspector::logs); logs whose first
/// topic matches the ERC20 `Transfer` signature and that carry the standard ERC20
/// layout are additionally decoded into [`transfers`](TransferInspector::transfers).
/// Logs that do not match (wrong signature, fewer than three topics, or fewer
/// than 32 data bytes) are retained in `logs` but produce no transfer.
impl<CTX, INTR> Inspector<CTX, INTR> for TransferInspector
where
    INTR: InterpreterTypes,
{
    /// Records `log` and, if it parses as an ERC20 `Transfer`, the decoded transfer.
    fn log(&mut self, _context: &mut CTX, log: Log) {
        // Try to parse as ERC20 Transfer event
        if let Some(transfer) = Self::parse_transfer(&log) {
            self.transfers.push(transfer);
        }

        // Store all logs for potential debugging/analysis
        self.logs.push(log);
    }
}

/// Parse ERC20 Transfer events from transaction receipt logs and compute
/// balance deltas for the given owner address.
///
/// This mirrors `TransferInspector::balance_deltas()` but operates on receipt
/// logs from an on-chain transaction rather than from an EVM inspector.
pub fn parse_receipt_deltas(receipt_logs: &[Log], owner: Address) -> HashMap<Address, I256> {
    let mut deltas: HashMap<Address, I256> = HashMap::new();

    for log in receipt_logs {
        if let Some(transfer) = TransferInspector::parse_transfer(log) {
            if transfer.from == owner {
                let entry = deltas.entry(transfer.token).or_insert(I256::ZERO);
                *entry -= I256::from_raw(transfer.value);
            }
            if transfer.to == owner {
                let entry = deltas.entry(transfer.token).or_insert(I256::ZERO);
                *entry += I256::from_raw(transfer.value);
            }
        }
    }

    deltas
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{address, bytes};

    fn make_transfer_log(token: Address, from: Address, to: Address, value: U256) -> Log {
        Log::new(
            token,
            vec![TRANSFER_EVENT_SIGNATURE, from.into_word(), to.into_word()],
            value.to_be_bytes_vec().into(),
        )
        .unwrap()
    }

    #[test]
    fn test_parse_single_transfer() {
        let token = address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"); // USDC
        let from = address!("1111111111111111111111111111111111111111");
        let to = address!("2222222222222222222222222222222222222222");
        let value = U256::from(1000000u64); // 1 USDC

        let log = make_transfer_log(token, from, to, value);
        let transfer = TransferInspector::parse_transfer(&log).expect("should parse");

        assert_eq!(transfer.token, token);
        assert_eq!(transfer.from, from);
        assert_eq!(transfer.to, to);
        assert_eq!(transfer.value, value);
    }

    #[test]
    fn test_balance_deltas_outgoing() {
        let mut inspector = TransferInspector::new();
        let token = address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
        let owner = address!("1111111111111111111111111111111111111111");
        let recipient = address!("2222222222222222222222222222222222222222");
        let value = U256::from(1000000u64);

        inspector.transfers.push(TokenTransfer {
            token,
            from: owner,
            to: recipient,
            value,
        });

        let deltas = inspector.balance_deltas(owner);
        assert_eq!(deltas.get(&token), Some(&(-I256::from_raw(value))));
    }

    #[test]
    fn test_balance_deltas_incoming() {
        let mut inspector = TransferInspector::new();
        let token = address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
        let sender = address!("1111111111111111111111111111111111111111");
        let owner = address!("2222222222222222222222222222222222222222");
        let value = U256::from(1000000u64);

        inspector.transfers.push(TokenTransfer {
            token,
            from: sender,
            to: owner,
            value,
        });

        let deltas = inspector.balance_deltas(owner);
        assert_eq!(deltas.get(&token), Some(&I256::from_raw(value)));
    }

    #[test]
    fn test_balance_deltas_multiple_transfers() {
        let mut inspector = TransferInspector::new();
        let token_a = address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
        let token_b = address!("C02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2");
        let owner = address!("1111111111111111111111111111111111111111");
        let other = address!("2222222222222222222222222222222222222222");

        // Owner sends 100 token_a
        inspector.transfers.push(TokenTransfer {
            token: token_a,
            from: owner,
            to: other,
            value: U256::from(100u64),
        });

        // Owner receives 50 token_b
        inspector.transfers.push(TokenTransfer {
            token: token_b,
            from: other,
            to: owner,
            value: U256::from(50u64),
        });

        // Owner receives 25 more token_a
        inspector.transfers.push(TokenTransfer {
            token: token_a,
            from: other,
            to: owner,
            value: U256::from(25u64),
        });

        let deltas = inspector.balance_deltas(owner);

        // token_a: -100 + 25 = -75
        assert_eq!(deltas.get(&token_a), Some(&I256::try_from(-75i64).unwrap()));
        // token_b: +50
        assert_eq!(deltas.get(&token_b), Some(&I256::try_from(50i64).unwrap()));
    }

    #[test]
    fn test_balance_deltas_for_tokens_filter() {
        let mut inspector = TransferInspector::new();
        let token_a = address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
        let token_b = address!("C02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2");
        let token_c = address!("6B175474E89094C44Da98b954EeecdE504D571D1");
        let owner = address!("1111111111111111111111111111111111111111");
        let other = address!("2222222222222222222222222222222222222222");

        inspector.transfers.push(TokenTransfer {
            token: token_a,
            from: owner,
            to: other,
            value: U256::from(100u64),
        });

        inspector.transfers.push(TokenTransfer {
            token: token_b,
            from: other,
            to: owner,
            value: U256::from(50u64),
        });

        inspector.transfers.push(TokenTransfer {
            token: token_c,
            from: other,
            to: owner,
            value: U256::from(200u64),
        });

        // Only request deltas for token_a and token_b
        let deltas = inspector.balance_deltas_for_tokens(owner, vec![token_a, token_b]);

        assert_eq!(deltas.len(), 2);
        assert!(deltas.contains_key(&token_a));
        assert!(deltas.contains_key(&token_b));
        assert!(!deltas.contains_key(&token_c));
    }

    #[test]
    fn test_non_transfer_log_ignored() {
        // Create a log with wrong signature
        let log = Log::new(
            address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
            vec![
                B256::ZERO, // Wrong signature
                address!("1111111111111111111111111111111111111111").into_word(),
                address!("2222222222222222222222222222222222222222").into_word(),
            ],
            U256::from(100u64).to_be_bytes_vec().into(),
        )
        .unwrap();

        assert!(TransferInspector::parse_transfer(&log).is_none());
    }

    #[test]
    fn test_inspector_clear() {
        let mut inspector = TransferInspector::new();
        inspector.transfers.push(TokenTransfer {
            token: Address::ZERO,
            from: Address::ZERO,
            to: Address::ZERO,
            value: U256::ZERO,
        });
        inspector
            .logs
            .push(Log::new_unchecked(Address::ZERO, vec![], bytes!("")));

        assert!(!inspector.transfers.is_empty());
        assert!(!inspector.logs.is_empty());

        inspector.clear();

        assert!(inspector.transfers.is_empty());
        assert!(inspector.logs.is_empty());
    }

    #[test]
    fn test_parse_receipt_deltas() {
        let token_a = address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
        let token_b = address!("C02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2");
        let owner = address!("1111111111111111111111111111111111111111");
        let other = address!("2222222222222222222222222222222222222222");

        let logs = vec![
            // Owner sends 100 token_a
            make_transfer_log(token_a, owner, other, U256::from(100u64)),
            // Owner receives 50 token_b
            make_transfer_log(token_b, other, owner, U256::from(50u64)),
            // Non-transfer log (wrong signature) - should be ignored
            Log::new(
                token_a,
                vec![B256::ZERO, owner.into_word(), other.into_word()],
                U256::from(999u64).to_be_bytes_vec().into(),
            )
            .unwrap(),
            // Owner receives 25 token_a
            make_transfer_log(token_a, other, owner, U256::from(25u64)),
        ];

        let deltas = super::parse_receipt_deltas(&logs, owner);

        // token_a: -100 + 25 = -75
        assert_eq!(deltas.get(&token_a), Some(&I256::try_from(-75i64).unwrap()));
        // token_b: +50
        assert_eq!(deltas.get(&token_b), Some(&I256::try_from(50i64).unwrap()));
    }

    #[test]
    fn test_parse_receipt_deltas_empty() {
        let owner = address!("1111111111111111111111111111111111111111");
        let deltas = super::parse_receipt_deltas(&[], owner);
        assert!(deltas.is_empty());
    }
}
