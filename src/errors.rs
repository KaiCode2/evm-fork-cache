//! Simulation error types and revert reason decoding.
//!
//! The module keeps EVM simulation failures structured: ordinary transaction
//! reverts are represented separately from infrastructure failures, and common
//! Solidity custom errors are decoded when their selectors are known.

use std::fmt;

use alloy_primitives::{Address, Bytes, FixedBytes, U256};
use alloy_sol_types::{SolError, sol};

sol! {
    #[derive(Debug)]
    error SwapFailed(address router, bytes data);

    #[derive(Debug)]
    error InvalidUniswapV3Swap();
    #[derive(Debug)]
    error InvalidUniswapV3SwapCallback();
    #[derive(Debug)]
    error InvalidUniswapV3Pool();
    #[derive(Debug)]
    error InvalidUniswapV2Swap();
    #[derive(Debug)]
    error InvalidUniswapV2Pool();
    #[derive(Debug)]
    error InvalidERC4626Deposit();
    #[derive(Debug)]
    error InvalidERC4626Redeem();

    #[derive(Debug)]
    error InvalidExecutionKind();
    #[derive(Debug)]
    error UnauthorizedFlashloanSender();
    #[derive(Debug)]
    error NoFee();
    #[derive(Debug)]
    error NoData();

    #[derive(Debug)]
    error NotCalm();

    #[derive(Debug)]
    error InsufficientBalance();

    #[derive(Debug)]
    error ERC20InsufficientBalance(address sender, uint256 balance, uint256 needed);
}

/// Known revert reasons decoded from raw EVM revert data.
#[derive(Debug, Clone)]
pub enum KnownRevertReason {
    /// A swap adapter reported that an underlying router call failed.
    SwapFailed {
        router: Address,
        call_data: Bytes,
    },
    InvalidUniswapV3Swap,
    InvalidUniswapV3SwapCallback,
    InvalidUniswapV3Pool,
    InvalidUniswapV2Swap,
    InvalidUniswapV2Pool,
    InvalidERC4626Deposit,
    InvalidERC4626Redeem,
    InvalidExecutionKind,
    UnauthorizedFlashloanSender,
    NoFee,
    NoData,
    /// A concentrated-liquidity calm-zone check failed.
    NotCalm,
    /// ERC20 transfer failed due to insufficient balance.
    InsufficientBalance,
    /// ERC20 transfer failed with IERC6093 details.
    ERC20InsufficientBalance {
        sender: Address,
        balance: U256,
        needed: U256,
    },
    /// Standard Solidity `Error(string)` revert.
    SolidityError(String),
    /// Standard Solidity `Panic(uint256)` revert.
    SolidityPanic(u64),
    /// Unknown selector and raw data.
    Unknown {
        selector: FixedBytes<4>,
        data: Bytes,
    },
}

impl fmt::Display for KnownRevertReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KnownRevertReason::SwapFailed { router, call_data } => {
                write!(
                    f,
                    "SwapFailed(router={}, data_len={})",
                    router,
                    call_data.len()
                )
            }
            KnownRevertReason::InvalidUniswapV3Swap => write!(f, "InvalidUniswapV3Swap"),
            KnownRevertReason::InvalidUniswapV3SwapCallback => {
                write!(f, "InvalidUniswapV3SwapCallback")
            }
            KnownRevertReason::InvalidUniswapV3Pool => write!(f, "InvalidUniswapV3Pool"),
            KnownRevertReason::InvalidUniswapV2Swap => write!(f, "InvalidUniswapV2Swap"),
            KnownRevertReason::InvalidUniswapV2Pool => write!(f, "InvalidUniswapV2Pool"),
            KnownRevertReason::InvalidERC4626Deposit => write!(f, "InvalidERC4626Deposit"),
            KnownRevertReason::InvalidERC4626Redeem => write!(f, "InvalidERC4626Redeem"),
            KnownRevertReason::InvalidExecutionKind => write!(f, "InvalidExecutionKind"),
            KnownRevertReason::UnauthorizedFlashloanSender => {
                write!(f, "UnauthorizedFlashloanSender")
            }
            KnownRevertReason::NoFee => write!(f, "NoFee"),
            KnownRevertReason::NoData => write!(f, "NoData"),
            KnownRevertReason::NotCalm => write!(f, "NotCalm"),
            KnownRevertReason::InsufficientBalance => write!(f, "InsufficientBalance"),
            KnownRevertReason::ERC20InsufficientBalance {
                sender,
                balance,
                needed,
            } => {
                write!(
                    f,
                    "ERC20InsufficientBalance(sender={}, balance={}, needed={})",
                    sender, balance, needed
                )
            }
            KnownRevertReason::SolidityError(msg) => write!(f, "Error(\"{}\")", msg),
            KnownRevertReason::SolidityPanic(code) => write!(f, "Panic({})", code),
            KnownRevertReason::Unknown { selector, data } => {
                write!(f, "Unknown(selector={}, data_len={})", selector, data.len())
            }
        }
    }
}

/// A structured simulation revert with decoded metadata when available.
#[derive(Debug, Clone)]
pub struct SimulationError {
    /// Gas used before the revert.
    pub gas_used: u64,
    /// Raw revert data returned by the EVM.
    pub revert_data: Bytes,
    /// Decoded revert reason, if recognized.
    pub reason: Option<KnownRevertReason>,
}

impl SimulationError {
    /// Create a simulation error from raw revert data.
    pub fn from_revert(gas_used: u64, output: Bytes) -> Self {
        let reason = decode_revert_reason(&output);
        Self {
            gas_used,
            revert_data: output,
            reason,
        }
    }

    /// Returns true for swap/router errors that usually invalidate a route.
    pub fn is_swap_failure(&self) -> bool {
        matches!(
            self.reason,
            Some(
                KnownRevertReason::SwapFailed { .. }
                    | KnownRevertReason::InvalidUniswapV3Swap
                    | KnownRevertReason::InvalidUniswapV3Pool
                    | KnownRevertReason::InvalidUniswapV2Swap
                    | KnownRevertReason::InvalidUniswapV2Pool
            )
        )
    }

    /// Returns true for the `NotCalm()` custom error selector.
    pub fn is_not_calm(&self) -> bool {
        matches!(self.reason, Some(KnownRevertReason::NotCalm))
    }

    /// Returns true when the revert indicates insufficient ERC20 balance.
    pub fn is_insufficient_balance(&self) -> bool {
        match &self.reason {
            Some(KnownRevertReason::InsufficientBalance) => true,
            Some(KnownRevertReason::ERC20InsufficientBalance { .. }) => true,
            Some(KnownRevertReason::SolidityError(msg)) => {
                msg.contains("transfer amount exceeds balance")
                    || msg.to_lowercase().contains("insufficient balance")
            }
            _ => false,
        }
    }
}

impl fmt::Display for SimulationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SimulationError(gas_used={}", self.gas_used)?;
        if let Some(reason) = &self.reason {
            write!(f, ", reason={}", reason)?;
        } else {
            write!(f, ", raw_data_len={}", self.revert_data.len())?;
        }
        write!(f, ")")
    }
}

impl std::error::Error for SimulationError {}

/// Decode EVM revert data into a known reason, if possible.
pub fn decode_revert_reason(data: &Bytes) -> Option<KnownRevertReason> {
    if data.len() < 4 {
        return None;
    }

    let selector: [u8; 4] = data[..4].try_into().ok()?;

    if let Ok(decoded) = SwapFailed::abi_decode(data) {
        return Some(KnownRevertReason::SwapFailed {
            router: decoded.router,
            call_data: decoded.data,
        });
    }

    if SwapFailed::SELECTOR == selector {
        return Some(KnownRevertReason::SwapFailed {
            router: Address::ZERO,
            call_data: data.slice(4..),
        });
    }

    if InvalidUniswapV3Swap::SELECTOR == selector {
        return Some(KnownRevertReason::InvalidUniswapV3Swap);
    }
    if InvalidUniswapV3SwapCallback::SELECTOR == selector {
        return Some(KnownRevertReason::InvalidUniswapV3SwapCallback);
    }
    if InvalidUniswapV3Pool::SELECTOR == selector {
        return Some(KnownRevertReason::InvalidUniswapV3Pool);
    }
    if InvalidUniswapV2Swap::SELECTOR == selector {
        return Some(KnownRevertReason::InvalidUniswapV2Swap);
    }
    if InvalidUniswapV2Pool::SELECTOR == selector {
        return Some(KnownRevertReason::InvalidUniswapV2Pool);
    }
    if InvalidERC4626Deposit::SELECTOR == selector {
        return Some(KnownRevertReason::InvalidERC4626Deposit);
    }
    if InvalidERC4626Redeem::SELECTOR == selector {
        return Some(KnownRevertReason::InvalidERC4626Redeem);
    }
    if InvalidExecutionKind::SELECTOR == selector {
        return Some(KnownRevertReason::InvalidExecutionKind);
    }
    if UnauthorizedFlashloanSender::SELECTOR == selector {
        return Some(KnownRevertReason::UnauthorizedFlashloanSender);
    }
    if NoFee::SELECTOR == selector {
        return Some(KnownRevertReason::NoFee);
    }
    if NoData::SELECTOR == selector {
        return Some(KnownRevertReason::NoData);
    }
    if NotCalm::SELECTOR == selector {
        return Some(KnownRevertReason::NotCalm);
    }
    if InsufficientBalance::SELECTOR == selector {
        return Some(KnownRevertReason::InsufficientBalance);
    }
    if let Ok(decoded) = ERC20InsufficientBalance::abi_decode(data) {
        return Some(KnownRevertReason::ERC20InsufficientBalance {
            sender: decoded.sender,
            balance: decoded.balance,
            needed: decoded.needed,
        });
    }

    if selector == [0x08, 0xc3, 0x79, 0xa0]
        && data.len() >= 68
        && let Ok(msg) = decode_solidity_error_string(data)
    {
        return Some(KnownRevertReason::SolidityError(msg));
    }

    if selector == [0x4e, 0x48, 0x7b, 0x71] && data.len() >= 36 {
        let code_bytes: [u8; 8] = data[28..36].try_into().ok()?;
        let code = u64::from_be_bytes(code_bytes);
        return Some(KnownRevertReason::SolidityPanic(code));
    }

    Some(KnownRevertReason::Unknown {
        selector: FixedBytes::from_slice(&selector),
        data: data.clone(),
    })
}

fn decode_solidity_error_string(data: &Bytes) -> Result<String, ()> {
    if data.len() < 68 {
        return Err(());
    }

    let length_start = 36;
    let length_bytes: [u8; 4] = data[length_start + 28..length_start + 32]
        .try_into()
        .map_err(|_| ())?;
    let length = u32::from_be_bytes(length_bytes) as usize;

    let string_start = 68;
    if data.len() < string_start + length {
        return Err(());
    }

    String::from_utf8(data[string_start..string_start + length].to_vec()).map_err(|_| ())
}

/// Result type for simulations that distinguish EVM reverts from host errors.
pub type SimulationResult<T> = Result<T, SimulationErrorKind>;

/// Error kind for simulation failures.
#[derive(Debug)]
pub enum SimulationErrorKind {
    /// The transaction reverted.
    Revert(Box<SimulationError>),
    /// An unexpected host-side error occurred.
    Other(anyhow::Error),
}

impl fmt::Display for SimulationErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SimulationErrorKind::Revert(e) => write!(f, "Revert: {}", e),
            SimulationErrorKind::Other(e) => write!(f, "Error: {}", e),
        }
    }
}

impl std::error::Error for SimulationErrorKind {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            SimulationErrorKind::Revert(e) => Some(e.as_ref()),
            SimulationErrorKind::Other(e) => e.source(),
        }
    }
}

impl From<anyhow::Error> for SimulationErrorKind {
    fn from(e: anyhow::Error) -> Self {
        SimulationErrorKind::Other(e)
    }
}

impl From<SimulationError> for SimulationErrorKind {
    fn from(e: SimulationError) -> Self {
        SimulationErrorKind::Revert(Box::new(e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build ABI-encoded revert data for a standard `Error(string)` revert.
    /// Layout: selector(4) | offset(32) | length(32) | utf8 bytes (padded).
    fn encode_solidity_error(message: &str) -> Bytes {
        let bytes = message.as_bytes();
        let mut out = Vec::new();
        out.extend_from_slice(&[0x08, 0xc3, 0x79, 0xa0]); // Error(string) selector

        // Offset to the string data (always 0x20 from the start of args).
        let mut offset = [0u8; 32];
        offset[31] = 0x20;
        out.extend_from_slice(&offset);

        // String length (assumed < 256 for these test fixtures).
        let mut length = [0u8; 32];
        length[31] = bytes.len() as u8;
        out.extend_from_slice(&length);

        // String bytes, right-padded to a 32-byte boundary.
        out.extend_from_slice(bytes);
        let pad = (32 - (bytes.len() % 32)) % 32;
        out.extend(std::iter::repeat_n(0u8, pad));

        Bytes::from(out)
    }

    #[test]
    fn decodes_solidity_error_string() {
        let data = encode_solidity_error("transfer amount exceeds balance");
        let reason = decode_revert_reason(&data).expect("should decode");
        match &reason {
            KnownRevertReason::SolidityError(msg) => {
                assert_eq!(msg, "transfer amount exceeds balance");
            }
            other => panic!("expected SolidityError, got {other:?}"),
        }

        // Routed through the public `SimulationError` constructor it should be
        // classified as an insufficient-balance failure.
        let err = SimulationError::from_revert(21_000, data);
        assert!(err.is_insufficient_balance());
        assert!(!err.is_swap_failure());
        assert!(!err.is_not_calm());
    }

    #[test]
    fn decodes_panic_uint256() {
        // selector(4) | uint256 panic code (0x11 = arithmetic overflow).
        let mut data = vec![0x4e, 0x48, 0x7b, 0x71];
        let mut code = [0u8; 32];
        code[31] = 0x11;
        data.extend_from_slice(&code);
        let data = Bytes::from(data);

        let reason = decode_revert_reason(&data).expect("should decode");
        assert!(matches!(reason, KnownRevertReason::SolidityPanic(0x11)));

        let err = SimulationError::from_revert(0, data);
        assert!(!err.is_swap_failure());
        assert!(!err.is_insufficient_balance());
    }

    #[test]
    fn decodes_known_custom_selector_not_calm() {
        let data = Bytes::from(NotCalm::SELECTOR.to_vec());
        let reason = decode_revert_reason(&data).expect("should decode");
        assert!(matches!(reason, KnownRevertReason::NotCalm));

        let err = SimulationError::from_revert(5_000, data);
        assert!(err.is_not_calm());
        assert!(!err.is_swap_failure());
        assert!(!err.is_insufficient_balance());
    }

    #[test]
    fn decodes_known_custom_selector_swap_failure() {
        let data = Bytes::from(InvalidUniswapV3Pool::SELECTOR.to_vec());
        let reason = decode_revert_reason(&data).expect("should decode");
        assert!(matches!(reason, KnownRevertReason::InvalidUniswapV3Pool));

        let err = SimulationError::from_revert(7_500, data);
        assert!(err.is_swap_failure());
    }

    #[test]
    fn decodes_insufficient_balance_custom_selector() {
        let data = Bytes::from(InsufficientBalance::SELECTOR.to_vec());
        let err = SimulationError::from_revert(0, data);
        assert!(err.is_insufficient_balance());
    }

    #[test]
    fn unknown_blob_is_classified_as_unknown() {
        // A 4-byte selector that matches no known error, plus trailing bytes.
        let data = Bytes::from(vec![0xde, 0xad, 0xbe, 0xef, 0x00, 0x01, 0x02, 0x03]);
        let reason = decode_revert_reason(&data).expect("should decode");
        match reason {
            KnownRevertReason::Unknown {
                selector,
                data: blob,
            } => {
                assert_eq!(selector.as_slice(), &[0xde, 0xad, 0xbe, 0xef]);
                assert_eq!(blob.len(), 8);
            }
            other => panic!("expected Unknown, got {other:?}"),
        }

        let err = SimulationError::from_revert(0, data);
        assert!(!err.is_swap_failure());
        assert!(!err.is_not_calm());
        assert!(!err.is_insufficient_balance());
    }

    #[test]
    fn data_shorter_than_selector_decodes_to_none() {
        let data = Bytes::from(vec![0x01, 0x02, 0x03]);
        assert!(decode_revert_reason(&data).is_none());

        let err = SimulationError::from_revert(0, data);
        assert!(err.reason.is_none());
        assert!(!err.is_insufficient_balance());
    }
}
