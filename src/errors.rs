//! Simulation error types and revert-reason decoding.
//!
//! Every EVM revert is either one of the two Solidity built-ins —
//! `Error(string)` (from `require`/`revert("msg")`) and `Panic(uint256)` (from
//! overflow, division-by-zero, etc.) — or a contract-defined *custom error*
//! identified by a 4-byte selector. This module decodes the two built-ins
//! natively and lets callers register any number of their own custom Solidity
//! errors with a [`RevertDecoder`].
//!
//! Application-specific selectors therefore live in the application, not in this
//! generic layer: define them with `sol!` and register them once.
//!
//! Note that [`Panic(uint256)`](RevertReason::Panic) codes that exceed
//! `u64::MAX` are dropped to `None` during decoding (and so surface as
//! [`RevertReason::Unknown`]). This is benign: real compiler-emitted panic
//! codes are single-byte constants (e.g. `0x11`, `0x32`).
//!
//! ```
//! use alloy_sol_types::{SolError, sol};
//! use evm_fork_cache::errors::{RevertDecoder, RevertReason};
//!
//! sol! {
//!     #[derive(Debug)]
//!     error Unauthorized(address caller);
//! }
//!
//! let decoder = RevertDecoder::new().with_error::<Unauthorized>();
//!
//! // 4-byte selector of `Unauthorized`, with no parameter bytes.
//! let raw = alloy_primitives::Bytes::from(Unauthorized::SELECTOR.to_vec());
//! match decoder.decode(&raw) {
//!     RevertReason::Custom(err) => assert_eq!(err.name, "Unauthorized(address)"),
//!     other => panic!("expected a custom error, got {other}"),
//! }
//! ```

use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use std::{borrow::Cow, io};

use alloy_primitives::{Address, B256, Bytes, FixedBytes, U256};
use alloy_sol_types::SolError;
use tracing::warn;

/// 4-byte selector of the standard Solidity `Error(string)` revert
/// (`0x08c379a0`), emitted by `require`/`revert("msg")`.
pub const ERROR_SELECTOR: [u8; 4] = [0x08, 0xc3, 0x79, 0xa0];

/// 4-byte selector of the standard Solidity `Panic(uint256)` revert
/// (`0x4e487b71`), emitted on overflow, division-by-zero, etc.
pub const PANIC_SELECTOR: [u8; 4] = [0x4e, 0x48, 0x7b, 0x71];

/// A decoded contract-defined custom error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CustomRevert {
    /// Human-readable signature, e.g. `"Unauthorized(address)"`.
    pub name: Cow<'static, str>,
    /// The error's 4-byte selector (the first 4 bytes of [`data`](Self::data)),
    /// the `keccak256` prefix of [`name`](Self::name).
    pub selector: FixedBytes<4>,
    /// Debug-formatted decoded parameters, when the body decoded successfully.
    ///
    /// `None` if only the selector matched but the ABI-encoded parameters could
    /// not be decoded (e.g. truncated revert data).
    pub params: Option<String>,
    /// Raw revert bytes (selector followed by ABI-encoded parameters).
    pub data: Bytes,
}

impl fmt::Display for CustomRevert {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.params {
            Some(params) => write!(f, "{params}"),
            None => write!(f, "{}", self.name),
        }
    }
}

/// A decoded EVM revert reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RevertReason {
    /// The call reverted with no return data (e.g. a bare `revert()` or `assert`
    /// in older Solidity, or an empty `require`).
    Empty,
    /// Standard Solidity `Error(string)` revert (e.g. `require(cond, "msg")`).
    Error(String),
    /// Standard Solidity `Panic(uint256)` revert (e.g. arithmetic overflow).
    Panic(u64),
    /// A contract-defined custom error whose selector was registered on the
    /// decoder via [`RevertDecoder::with_error`], [`RevertDecoder::register`],
    /// or [`RevertDecoder::register_raw`].
    Custom(CustomRevert),
    /// A selector that matched no built-in or registered custom error.
    Unknown {
        /// The 4-byte selector (right-padded with zeros if fewer than 4 bytes
        /// of revert data were returned).
        selector: FixedBytes<4>,
        /// Raw revert bytes.
        data: Bytes,
    },
}

impl fmt::Display for RevertReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RevertReason::Empty => write!(f, "<empty revert>"),
            RevertReason::Error(msg) => write!(f, "Error({msg:?})"),
            RevertReason::Panic(code) => write!(f, "Panic({code:#x})"),
            RevertReason::Custom(custom) => write!(f, "{custom}"),
            RevertReason::Unknown { selector, data } => {
                write!(f, "Unknown(selector={selector}, data_len={})", data.len())
            }
        }
    }
}

type DecodeFn = Arc<dyn Fn(&Bytes) -> Option<String> + Send + Sync>;

#[derive(Clone)]
struct CustomErrorDecoder {
    name: Cow<'static, str>,
    decode: DecodeFn,
}

/// Error returned when registering a custom error selector that already exists.
///
/// A [`RevertDecoder`] keeps the first decoder registered for a selector so a
/// later registration cannot silently change how existing revert data decodes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DuplicateSelectorError {
    /// The 4-byte selector that was already registered.
    pub selector: FixedBytes<4>,
    /// Signature/name of the existing registration that will be kept.
    pub existing: Cow<'static, str>,
    /// Signature/name of the attempted duplicate registration.
    pub attempted: Cow<'static, str>,
}

impl fmt::Display for DuplicateSelectorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "duplicate custom error selector {}: keeping {}, ignoring {}",
            self.selector, self.existing, self.attempted
        )
    }
}

impl std::error::Error for DuplicateSelectorError {}

/// Decodes raw EVM revert data into a [`RevertReason`].
///
/// The two standard Solidity built-ins — `Error(string)` and `Panic(uint256)` —
/// are always recognized. Register additional contract-defined custom errors
/// with [`with_error`](RevertDecoder::with_error),
/// [`register`](RevertDecoder::register), or
/// [`register_raw`](RevertDecoder::register_raw). Duplicate custom-error
/// selectors keep the first registration; use
/// [`try_register`](RevertDecoder::try_register) or
/// [`try_register_raw`](RevertDecoder::try_register_raw) when collisions should
/// be handled as errors instead of warnings.
///
/// The decoder is cheap to [`Clone`] and is `Send + Sync`, so a configured
/// decoder can be shared across parallel simulations.
#[derive(Clone, Default)]
pub struct RevertDecoder {
    custom: HashMap<[u8; 4], CustomErrorDecoder>,
}

impl fmt::Debug for RevertDecoder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut names: Vec<&str> = self.custom.values().map(|d| d.name.as_ref()).collect();
        names.sort_unstable();
        f.debug_struct("RevertDecoder")
            .field("custom_errors", &names)
            .finish()
    }
}

impl RevertDecoder {
    /// Create a decoder that recognizes only the standard Solidity built-ins
    /// (`Error(string)` and `Panic(uint256)`) and no custom errors.
    ///
    /// ```
    /// use evm_fork_cache::errors::RevertDecoder;
    ///
    /// let decoder = RevertDecoder::new();
    /// assert!(decoder.is_empty());
    /// ```
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a `sol!`-generated custom error type for decoding, consuming and
    /// returning `self` for builder-style chaining.
    ///
    /// If the selector is already registered, the first registration is kept
    /// and a warning is emitted. Use [`try_register`](Self::try_register) when
    /// duplicate selectors should fail configuration.
    ///
    /// ```
    /// use alloy_sol_types::sol;
    /// use evm_fork_cache::errors::RevertDecoder;
    ///
    /// sol! {
    ///     #[derive(Debug)]
    ///     error SlippageExceeded(uint256 wanted, uint256 got);
    ///     #[derive(Debug)]
    ///     error Paused();
    /// }
    ///
    /// let decoder = RevertDecoder::new()
    ///     .with_error::<SlippageExceeded>()
    ///     .with_error::<Paused>();
    /// assert_eq!(decoder.len(), 2);
    /// ```
    pub fn with_error<E>(mut self) -> Self
    where
        E: SolError + fmt::Debug + 'static,
    {
        self.register::<E>();
        self
    }

    /// Register a `sol!`-generated custom error type for decoding.
    ///
    /// If an error with the same selector is already registered, the first
    /// registration is kept and a warning is emitted. Use
    /// [`try_register`](Self::try_register) to surface duplicates as errors.
    pub fn register<E>(&mut self) -> &mut Self
    where
        E: SolError + fmt::Debug + 'static,
    {
        if let Err(err) = self.try_register::<E>() {
            warn_duplicate_selector(&err);
        }
        self
    }

    /// Register a `sol!`-generated custom error type for decoding, returning an
    /// error when another custom error already owns the same selector.
    pub fn try_register<E>(&mut self) -> Result<&mut Self, DuplicateSelectorError>
    where
        E: SolError + fmt::Debug + 'static,
    {
        let decode: DecodeFn =
            Arc::new(|data: &Bytes| E::abi_decode(data).ok().map(|err| format!("{err:?}")));
        self.insert_custom_error(
            E::SELECTOR,
            CustomErrorDecoder {
                name: Cow::Borrowed(E::SIGNATURE),
                decode,
            },
        )
    }

    /// Register a custom error by raw selector, name, and parameter decoder.
    ///
    /// Use this when there is no `sol!`-generated type to hand — for example
    /// when the selector and signature come from an ABI loaded at runtime. The
    /// `decode` closure receives the full revert bytes (selector included) and
    /// returns the formatted parameters, or `None` if it cannot decode them.
    ///
    /// If the closure returns `None`, the selector still matches: the decode
    /// yields a [`RevertReason::Custom`] whose
    /// [`params`](CustomRevert::params) is `None`. If an error with the same
    /// selector is already registered, the first registration is kept and a
    /// warning is emitted. Use [`try_register_raw`](Self::try_register_raw) to
    /// surface duplicates as errors.
    ///
    /// ```
    /// use alloy_primitives::Bytes;
    /// use evm_fork_cache::errors::{RevertDecoder, RevertReason};
    ///
    /// let mut decoder = RevertDecoder::new();
    /// // A closure that decodes the parameters when there is a payload byte,
    /// // and otherwise reports a decode failure by returning `None`.
    /// decoder.register_raw([0xde, 0xad, 0xbe, 0xef], "MyError(uint256)", |data| {
    ///     (data.len() > 4).then(|| format!("payload {} bytes", data.len() - 4))
    /// });
    ///
    /// // Selector plus a payload byte: the closure decodes the parameters.
    /// let with_params = Bytes::from(vec![0xde, 0xad, 0xbe, 0xef, 0x00]);
    /// match decoder.decode(&with_params) {
    ///     RevertReason::Custom(custom) => {
    ///         assert_eq!(custom.name, "MyError(uint256)");
    ///         assert_eq!(custom.params.as_deref(), Some("payload 1 bytes"));
    ///     }
    ///     other => panic!("expected Custom, got {other}"),
    /// }
    ///
    /// // Bare selector: the closure returns `None`, but the selector still
    /// // matches, so the result is a `Custom` with `params == None`.
    /// let bare = Bytes::from(vec![0xde, 0xad, 0xbe, 0xef]);
    /// match decoder.decode(&bare) {
    ///     RevertReason::Custom(custom) => assert!(custom.params.is_none()),
    ///     other => panic!("expected Custom, got {other}"),
    /// }
    /// ```
    pub fn register_raw(
        &mut self,
        selector: [u8; 4],
        name: impl Into<Cow<'static, str>>,
        decode: impl Fn(&Bytes) -> Option<String> + Send + Sync + 'static,
    ) -> &mut Self {
        if let Err(err) = self.try_register_raw(selector, name, decode) {
            warn_duplicate_selector(&err);
        }
        self
    }

    /// Register a custom error by raw selector, name, and parameter decoder,
    /// returning an error when another custom error already owns the selector.
    ///
    /// The `decode` closure receives the full revert bytes (selector included)
    /// and returns formatted parameters, or `None` if the selector matched but
    /// the parameter payload could not be decoded.
    pub fn try_register_raw(
        &mut self,
        selector: [u8; 4],
        name: impl Into<Cow<'static, str>>,
        decode: impl Fn(&Bytes) -> Option<String> + Send + Sync + 'static,
    ) -> Result<&mut Self, DuplicateSelectorError> {
        self.insert_custom_error(
            selector,
            CustomErrorDecoder {
                name: name.into(),
                decode: Arc::new(decode),
            },
        )
    }

    /// Number of registered custom errors. The two Solidity built-ins are
    /// always recognized and are not counted, so a freshly
    /// [`new`](RevertDecoder::new) decoder reports `0`.
    pub fn len(&self) -> usize {
        self.custom.len()
    }

    /// Returns `true` if no custom errors are registered. The built-ins are
    /// always recognized regardless, so this is `true` for a freshly
    /// [`new`](RevertDecoder::new) decoder.
    pub fn is_empty(&self) -> bool {
        self.custom.is_empty()
    }

    /// Decode raw EVM revert data into a [`RevertReason`].
    ///
    /// Resolution order: the two Solidity built-ins (`Error(string)` and
    /// `Panic(uint256)`), then registered custom errors by selector, then
    /// [`RevertReason::Unknown`] for anything else. Empty input decodes to
    /// [`RevertReason::Empty`], and data shorter than 4 bytes decodes to
    /// [`RevertReason::Unknown`] with the selector right-padded with zeros.
    ///
    /// ```
    /// use alloy_primitives::{Bytes, U256};
    /// use alloy_sol_types::{Panic, SolError, sol};
    /// use evm_fork_cache::errors::{RevertDecoder, RevertReason, ERROR_SELECTOR};
    ///
    /// sol! {
    ///     #[derive(Debug)]
    ///     error Custom();
    /// }
    ///
    /// let decoder = RevertDecoder::new().with_error::<Custom>();
    ///
    /// // Built-in `Error(string)` decodes natively, without registration.
    /// // Layout: selector | offset(0x20) | length | utf8 bytes (padded).
    /// let mut bytes = ERROR_SELECTOR.to_vec();
    /// bytes.extend_from_slice(&{ let mut o = [0u8; 32]; o[31] = 0x20; o }); // offset
    /// bytes.extend_from_slice(&{ let mut l = [0u8; 32]; l[31] = 2; l });    // length 2
    /// bytes.extend_from_slice(b"hi");
    /// bytes.extend_from_slice(&[0u8; 30]);                                  // pad to 32
    /// assert_eq!(decoder.decode(&Bytes::from(bytes)), RevertReason::Error("hi".into()));
    ///
    /// // Built-in `Panic(uint256)` decodes natively too.
    /// let panic = Bytes::from(Panic { code: U256::from(0x11) }.abi_encode());
    /// assert_eq!(decoder.decode(&panic), RevertReason::Panic(0x11));
    ///
    /// // A registered selector resolves to `Custom`.
    /// let raw = Bytes::from(Custom::SELECTOR.to_vec());
    /// match decoder.decode(&raw) {
    ///     RevertReason::Custom(err) => assert_eq!(err.name, "Custom()"),
    ///     other => panic!("expected Custom, got {other}"),
    /// }
    ///
    /// // An unregistered selector falls through to `Unknown`.
    /// let unknown = Bytes::from(vec![0xde, 0xad, 0xbe, 0xef]);
    /// assert!(matches!(decoder.decode(&unknown), RevertReason::Unknown { .. }));
    /// ```
    pub fn decode(&self, data: &Bytes) -> RevertReason {
        if data.is_empty() {
            return RevertReason::Empty;
        }
        if data.len() < 4 {
            // Too short for a selector; surface the raw bytes as Unknown with a
            // right-padded selector so nothing is silently discarded.
            let mut selector = [0u8; 4];
            selector[..data.len()].copy_from_slice(&data[..]);
            return RevertReason::Unknown {
                selector: FixedBytes::from(selector),
                data: data.clone(),
            };
        }

        let selector: [u8; 4] = data[..4].try_into().expect("length checked >= 4");

        if selector == ERROR_SELECTOR
            && let Some(message) = decode_solidity_error_string(data)
        {
            return RevertReason::Error(message);
        }
        if selector == PANIC_SELECTOR
            && let Some(code) = decode_solidity_panic(data)
        {
            return RevertReason::Panic(code);
        }
        if let Some(entry) = self.custom.get(&selector) {
            return RevertReason::Custom(CustomRevert {
                name: entry.name.clone(),
                selector: FixedBytes::from(selector),
                params: (entry.decode)(data),
                data: data.clone(),
            });
        }

        RevertReason::Unknown {
            selector: FixedBytes::from(selector),
            data: data.clone(),
        }
    }

    fn insert_custom_error(
        &mut self,
        selector: [u8; 4],
        decoder: CustomErrorDecoder,
    ) -> Result<&mut Self, DuplicateSelectorError> {
        if let Some(existing) = self.custom.get(&selector) {
            return Err(DuplicateSelectorError {
                selector: FixedBytes::from(selector),
                existing: existing.name.clone(),
                attempted: decoder.name,
            });
        }

        self.custom.insert(selector, decoder);
        Ok(self)
    }
}

fn warn_duplicate_selector(err: &DuplicateSelectorError) {
    warn!(
        selector = %err.selector,
        existing = err.existing.as_ref(),
        attempted = err.attempted.as_ref(),
        "duplicate custom error selector registration ignored; keeping first registration"
    );
}

/// Decode revert data using only the standard Solidity built-ins.
///
/// For application-specific custom errors, build a [`RevertDecoder`] and call
/// [`RevertDecoder::decode`].
pub fn decode_revert_reason(data: &Bytes) -> RevertReason {
    static STANDARD: OnceLock<RevertDecoder> = OnceLock::new();
    STANDARD.get_or_init(RevertDecoder::new).decode(data)
}

/// Decode the `uint256` payload of a standard `Panic(uint256)` revert.
///
/// Delegates to alloy's built-in decoder (which validates the ABI encoding) and
/// returns `None` for codes that do not fit in a `u64`. Real compiler-emitted
/// panic codes are single-byte constants (e.g. `0x11` for arithmetic overflow,
/// `0x32` for out-of-bounds array access).
fn decode_solidity_panic(data: &Bytes) -> Option<u64> {
    alloy_sol_types::Panic::abi_decode(data)
        .ok()
        .and_then(|panic| u64::try_from(panic.code).ok())
}

/// Decode the string payload of a standard `Error(string)` revert.
///
/// Delegates to alloy's built-in decoder, which follows the ABI offset and
/// validates the length rather than assuming a fixed in-memory layout — so it
/// stays correct on non-standard or adversarial revert data.
fn decode_solidity_error_string(data: &Bytes) -> Option<String> {
    alloy_sol_types::Revert::abi_decode(data)
        .ok()
        .map(|revert| revert.reason)
}

/// A structured simulation revert with its decoded reason.
#[derive(Debug, Clone)]
pub struct SimulationError {
    /// Gas consumed before the revert.
    pub gas_used: u64,
    /// Raw revert data returned by the EVM (the bytes that were decoded into
    /// [`reason`](Self::reason)).
    pub revert_data: Bytes,
    /// The revert reason decoded from [`revert_data`](Self::revert_data).
    pub reason: RevertReason,
}

impl SimulationError {
    /// Create a simulation error from raw revert data, decoding with the
    /// standard Solidity built-ins only.
    pub fn from_revert(gas_used: u64, output: Bytes) -> Self {
        let reason = decode_revert_reason(&output);
        Self {
            gas_used,
            revert_data: output,
            reason,
        }
    }

    /// Create a simulation error from raw revert data, decoding custom errors
    /// with the supplied [`RevertDecoder`].
    pub fn from_revert_with(gas_used: u64, output: Bytes, decoder: &RevertDecoder) -> Self {
        let reason = decoder.decode(&output);
        Self {
            gas_used,
            revert_data: output,
            reason,
        }
    }

    /// The decoded revert reason. Equivalent to borrowing the public
    /// [`reason`](Self::reason) field.
    pub fn reason(&self) -> &RevertReason {
        &self.reason
    }

    /// The `Error(string)` message, if this was a standard string revert.
    pub fn revert_message(&self) -> Option<&str> {
        match &self.reason {
            RevertReason::Error(message) => Some(message.as_str()),
            _ => None,
        }
    }

    /// The panic code, if this was a standard `Panic(uint256)` revert.
    pub fn panic_code(&self) -> Option<u64> {
        match self.reason {
            RevertReason::Panic(code) => Some(code),
            _ => None,
        }
    }

    /// The decoded custom error, if a registered custom error matched.
    pub fn custom_error(&self) -> Option<&CustomRevert> {
        match &self.reason {
            RevertReason::Custom(custom) => Some(custom),
            _ => None,
        }
    }

    /// The 4-byte selector of the revert, if any (custom or unknown).
    pub fn selector(&self) -> Option<FixedBytes<4>> {
        match &self.reason {
            RevertReason::Custom(custom) => Some(custom.selector),
            RevertReason::Unknown { selector, .. } => Some(*selector),
            _ => None,
        }
    }

    /// `true` if the call reverted with no return data, i.e. the reason is
    /// [`RevertReason::Empty`].
    pub fn is_empty_revert(&self) -> bool {
        matches!(self.reason, RevertReason::Empty)
    }
}

impl fmt::Display for SimulationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SimulationError(gas_used={}, reason={})",
            self.gas_used, self.reason
        )
    }
}

impl std::error::Error for SimulationError {}

/// Error raised when validating or refreshing the block-execution context
/// (`NUMBER` / `BASEFEE` / `COINBASE` / `PREVRANDAO` / `GASLIMIT` opcodes).
///
/// Under strict [`BlockContextRequirements`](crate::cache::BlockContextRequirements)
/// a missing header field surfaces loudly as one of these variants instead of
/// silently defaulting the corresponding EVM block-env field.
#[derive(Debug, thiserror::Error)]
pub enum BlockContextError {
    /// The block header could not be fetched (the provider errored or returned
    /// no block) while strict requirements demanded one at construction.
    #[error("block-context header fetch failed: {0}")]
    FetchFailed(String),
    /// A header was available but a required block-context field was absent.
    ///
    /// `field` is the lowercased field token (e.g. `"basefee"`, `"prevrandao"`).
    #[error("required block-context field missing: {field}")]
    MissingField {
        /// The lowercased name of the missing field.
        field: &'static str,
    },
}

/// Error returned when crate-managed synchronous RPC bridges cannot enter the
/// required tokio runtime context.
#[derive(Debug, Clone, thiserror::Error)]
pub enum RuntimeError {
    /// A current-thread runtime was active, but the crate needs
    /// `tokio::task::block_in_place`, which is available only on multi-thread
    /// runtimes.
    #[error(
        "evm-fork-cache RPC operations require a multi-thread tokio runtime; \
         found a current-thread runtime (block_in_place is not supported there). \
         Build the runtime with `tokio::runtime::Builder::new_multi_thread()` \
         or annotate with `#[tokio::main(flavor = \"multi_thread\")]`"
    )]
    CurrentThreadRuntime,
    /// No usable runtime handle was available.
    #[error(
        "evm-fork-cache RPC operations require a running multi-thread tokio runtime: {details}"
    )]
    MissingRuntime {
        /// The runtime lookup error rendered by tokio.
        details: String,
    },
}

/// Error returned by direct RPC call callbacks installed on [`EvmCache`].
///
/// [`EvmCache`]: crate::cache::EvmCache
#[derive(Debug, thiserror::Error)]
pub enum RpcError {
    /// Runtime precondition failure before the RPC call could be made.
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
    /// The provider rejected or failed the RPC request.
    #[error("RPC provider call {operation} failed: {details}")]
    Provider {
        /// JSON-RPC method or high-level operation name.
        operation: &'static str,
        /// Provider/transport error text. Alloy's provider error type is generic
        /// over the transport, so this boundary preserves it as display text.
        details: String,
    },
    /// Caller-provided callback failure.
    #[error("custom RPC callback failed: {message}")]
    Custom {
        /// Caller-provided error text.
        message: String,
    },
}

impl RpcError {
    /// Build a provider-failure error from any displayable provider error.
    pub fn provider(operation: &'static str, source: impl fmt::Display) -> Self {
        Self::Provider {
            operation,
            details: source.to_string(),
        }
    }

    /// Build a custom-callback error.
    pub fn custom(message: impl Into<String>) -> Self {
        Self::Custom {
            message: message.into(),
        }
    }
}

/// Error returned by storage/proof batch callbacks.
///
/// `Clone` lets a batch-level failure (one `eth_call` or one JSON-RPC batch)
/// be reported once per affected slot without re-stringifying the source.
#[derive(Debug, Clone, thiserror::Error)]
pub enum StorageFetchError {
    /// Runtime precondition failure before a fetch could be made.
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
    /// The provider rejected or failed an RPC request.
    #[error("storage/provider request {operation} failed: {details}")]
    Provider {
        /// JSON-RPC method or high-level operation name.
        operation: &'static str,
        /// Provider/transport error text. Alloy's provider error type is generic
        /// over the transport, so this boundary preserves it as display text.
        details: String,
    },
    /// JSON-RPC batch serialization failed before sending.
    #[error("failed to serialize storage batch request: {details}")]
    Serialization {
        /// Serialization error text.
        details: String,
    },
    /// Sending the JSON-RPC batch failed before per-call waiters could resolve.
    #[error("failed to send storage batch request: {details}")]
    BatchSend {
        /// Send error text.
        details: String,
    },
    /// Caller-provided callback failure.
    #[error("custom storage/proof fetcher failed: {message}")]
    Custom {
        /// Caller-provided error text.
        message: String,
    },
}

impl StorageFetchError {
    /// Build a provider-failure error from any displayable provider error.
    pub fn provider(operation: &'static str, source: impl fmt::Display) -> Self {
        Self::Provider {
            operation,
            details: source.to_string(),
        }
    }

    /// Build a batch-send error.
    pub fn batch_send(source: impl fmt::Display) -> Self {
        Self::BatchSend {
            details: source.to_string(),
        }
    }

    /// Build a serialization error.
    pub fn serialization(source: impl fmt::Display) -> Self {
        Self::Serialization {
            details: source.to_string(),
        }
    }

    /// Build a custom-callback error.
    pub fn custom(message: impl Into<String>) -> Self {
        Self::Custom {
            message: message.into(),
        }
    }
}

/// Result type returned by storage/proof batch callbacks.
pub type StorageFetchResult<T> = Result<T, StorageFetchError>;

/// Persistence failure for crate-owned on-disk cache files.
#[derive(Debug, thiserror::Error)]
pub enum PersistenceError {
    /// bincode serialization failed.
    #[error("failed to serialize {label}: {source}")]
    Serialize {
        /// Human-readable cache payload label.
        label: &'static str,
        /// bincode serialization failure.
        #[source]
        source: bincode::Error,
    },
    /// A parent directory could not be created.
    #[error("failed to create directory {path:?}: {source}")]
    CreateDir {
        /// Directory path.
        path: PathBuf,
        /// Filesystem error.
        #[source]
        source: io::Error,
    },
    /// A file write failed.
    #[error("failed to write {path:?}: {source}")]
    Write {
        /// File path.
        path: PathBuf,
        /// Filesystem error.
        #[source]
        source: io::Error,
    },
}

impl PersistenceError {
    /// Build a bincode serialization error.
    pub(crate) fn serialize(label: &'static str, source: bincode::Error) -> Self {
        Self::Serialize { label, source }
    }

    /// Build a directory creation error.
    pub(crate) fn create_dir(path: impl Into<PathBuf>, source: io::Error) -> Self {
        Self::CreateDir {
            path: path.into(),
            source,
        }
    }

    /// Build a file write error.
    pub(crate) fn write(path: impl Into<PathBuf>, source: io::Error) -> Self {
        Self::Write {
            path: path.into(),
            source,
        }
    }
}

/// General cache-operation error for APIs that execute or mutate local fork
/// state but do not classify EVM reverts as [`SimError`].
#[derive(Debug, thiserror::Error)]
pub enum CacheError {
    /// Runtime precondition failure.
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
    /// Direct RPC callback failure.
    #[error(transparent)]
    Rpc(#[from] RpcError),
    /// On-disk persistence failure.
    #[error(transparent)]
    Persistence(#[from] PersistenceError),
    /// A storage verification/reconciliation path had no batch fetcher.
    #[error("storage verification requires a storage batch fetcher")]
    MissingStorageBatchFetcher,
    /// Code-seed verification had pending seeds but no account-fields fetcher.
    #[error("code-seed verification requires an account fields fetcher")]
    MissingAccountFieldsFetcher,
    /// Reconciliation could not fetch any requested slot, so it cannot prove the
    /// local state fresh.
    #[error(
        "reconcile could not fetch any of the {requested} requested slot(s) \
         (no usable storage fetcher / provider unreachable)"
    )]
    ReconcileFetchFailed {
        /// Number of requested slots.
        requested: usize,
    },
    /// Account fetch failed.
    #[error("failed to fetch account {address}: {details}")]
    AccountFetch {
        /// Account address.
        address: Address,
        /// Backend/provider error text.
        details: String,
    },
    /// A canonical code seed contradicts code already fetched from the chain.
    ///
    /// Chain-fetched state is authoritative over templates: the seed is
    /// rejected and the cached code is left untouched. If the caller believes
    /// the chain has moved (e.g. a redeploy), purge the account first and
    /// re-seed.
    #[error(
        "code seed for {address} conflicts with chain-fetched code \
         (cached hash {cached}, seeded hash {seeded})"
    )]
    CodeSeedConflict {
        /// Account address.
        address: Address,
        /// Code hash of the RPC-origin code already in the cache.
        cached: B256,
        /// Code hash of the rejected seed.
        seeded: B256,
    },
    /// A code seed/etch was given empty bytes; claiming an address is
    /// code-less is not a supported seed (that is what verification's
    /// `codeless` classification reports).
    #[error("cannot seed/etch empty code at {address}")]
    CodeSeedEmpty {
        /// Account address.
        address: Address,
    },
    /// Storage read failed.
    #[error("storage read failed for {address} slot {slot}: {details}")]
    StorageRead {
        /// Contract address.
        address: Address,
        /// Storage slot.
        slot: U256,
        /// Backend/provider error text.
        details: String,
    },
    /// Storage insert failed.
    #[error("storage insert failed for {address} slot {slot}: {details}")]
    StorageInsert {
        /// Contract address.
        address: Address,
        /// Storage slot.
        slot: U256,
        /// Backend error text.
        details: String,
    },
    /// Transaction environment construction failed.
    #[error("failed to build transaction environment: {details}")]
    TxEnv {
        /// Builder error text.
        details: String,
    },
    /// revm returned a host/database transaction error.
    #[error("failed to transact: {details}")]
    Transact {
        /// revm/database error text.
        details: String,
    },
    /// A helper that requires a successful EVM call observed a revert or halt.
    #[error("EVM call did not succeed: {result}")]
    CallNotSuccessful {
        /// Debug rendering of the execution result.
        result: String,
    },
    /// A debug/trace RPC response could not be parsed into the cache's typed
    /// block state-diff representation.
    #[error("failed to parse block state trace: {details}")]
    TraceParse {
        /// Parser error text.
        details: String,
    },
    /// ABI or helper-specific decode failure.
    #[error("failed to decode {what}: {details}")]
    Decode {
        /// Human-readable decode target.
        what: &'static str,
        /// Decoder error text.
        details: String,
    },
    /// A typed Solidity call executed but did not succeed.
    #[error("Solidity call {signature} from {from:?} to {to:?} did not succeed: {result}")]
    SolCallFailed {
        /// Solidity function signature.
        signature: &'static str,
        /// Call sender.
        from: Address,
        /// Call target.
        to: Address,
        /// Debug rendering of the execution result.
        result: String,
    },
    /// A typed Solidity call returned malformed data.
    #[error(
        "failed to decode Solidity call {signature} return data from {from:?} to {to:?}: \
         output_len={output_len}, error: {details}"
    )]
    SolCallDecode {
        /// Solidity function signature.
        signature: &'static str,
        /// Call sender.
        from: Address,
        /// Call target.
        to: Address,
        /// Return-data length in bytes.
        output_len: usize,
        /// Decoder error text.
        details: String,
    },
    /// Deployment succeeded without a created address.
    #[error("contract deployment succeeded but no address returned")]
    DeploymentMissingAddress,
    /// Deployment reverted.
    #[error("contract deployment reverted: 0x{output_hex}")]
    DeploymentReverted {
        /// Hex-encoded revert output.
        output_hex: String,
    },
    /// Deployment halted.
    #[error("contract deployment halted: {reason}")]
    DeploymentHalted {
        /// Debug rendering of the halt reason.
        reason: String,
    },
    /// Source account did not contain bytecode for an override.
    #[error("no bytecode found at source address {source_address}")]
    MissingSourceBytecode {
        /// Source address.
        source_address: Address,
    },
    /// Runtime bytecode was required but absent or empty.
    #[error("{role} account {address} has no runtime bytecode")]
    MissingRuntimeCode {
        /// Account role in the operation.
        role: &'static str,
        /// Account address.
        address: Address,
    },
    /// Target account was required but absent.
    #[error(
        "target account {target} not found; use override_or_create_account_code for synthetic targets"
    )]
    MissingTargetAccount {
        /// Target address.
        target: Address,
    },
    /// Target account fetch failed while validating an override target.
    #[error("failed to fetch target account {target}: {details}")]
    TargetAccountFetch {
        /// Target address.
        target: Address,
        /// Backend/provider error text.
        details: String,
    },
}

impl CacheError {
    /// Convert a transaction-builder error into [`CacheError::TxEnv`].
    pub fn tx_env(source: impl fmt::Debug) -> Self {
        Self::TxEnv {
            details: format!("{source:?}"),
        }
    }

    /// Convert a revm host/database transaction error into
    /// [`CacheError::Transact`].
    pub fn transact(source: impl fmt::Debug) -> Self {
        Self::Transact {
            details: format!("{source:?}"),
        }
    }
}

/// Result type returned by cache APIs.
pub type CacheResult<T, E = CacheError> = Result<T, E>;

/// Error for immutable snapshot-overlay execution helpers that do not classify
/// reverts as [`SimError`].
#[derive(Debug, thiserror::Error)]
pub enum OverlayError {
    /// Transaction environment construction failed.
    #[error("failed to build transaction environment: {details}")]
    TxEnv {
        /// Builder error text.
        details: String,
    },
    /// revm returned a host/database transaction error.
    #[error("failed to transact: {details}")]
    Transact {
        /// revm/database error text.
        details: String,
    },
}

impl OverlayError {
    /// Convert a transaction-builder error into [`OverlayError::TxEnv`].
    pub fn tx_env(source: impl fmt::Debug) -> Self {
        Self::TxEnv {
            details: format!("{source:?}"),
        }
    }

    /// Convert a revm host/database transaction error into
    /// [`OverlayError::Transact`].
    pub fn transact(source: impl fmt::Debug) -> Self {
        Self::Transact {
            details: format!("{source:?}"),
        }
    }
}

/// Result type returned by overlay APIs that return raw [`ExecutionResult`]
/// values instead of classifying reverts.
///
/// [`ExecutionResult`]: revm::context::result::ExecutionResult
pub type OverlayResult<T, E = OverlayError> = Result<T, E>;

/// Host-side failure for simulation entry points.
#[derive(Debug, thiserror::Error)]
pub enum SimHostError {
    /// Transaction environment construction failed.
    #[error("failed to build transaction environment: {details}")]
    TxEnv {
        /// Builder error text.
        details: String,
    },
    /// revm returned a host/database transaction error.
    #[error("failed to transact: {details}")]
    Transact {
        /// revm/database error text.
        details: String,
    },
    /// Database read failed outside a transaction execution.
    #[error("database operation failed: {details}")]
    Database {
        /// Database error text.
        details: String,
    },
    /// Cache helper failed.
    #[error(transparent)]
    Cache(#[from] CacheError),
    /// Overlay helper failed.
    #[error(transparent)]
    Overlay(#[from] OverlayError),
}

impl SimHostError {
    /// Convert a transaction-builder error into [`SimHostError::TxEnv`].
    pub fn tx_env(source: impl fmt::Debug) -> Self {
        Self::TxEnv {
            details: format!("{source:?}"),
        }
    }

    /// Convert a revm host/database transaction error into
    /// [`SimHostError::Transact`].
    pub fn transact(source: impl fmt::Debug) -> Self {
        Self::Transact {
            details: format!("{source:?}"),
        }
    }

    /// Convert a database error into [`SimHostError::Database`].
    pub fn database(source: impl fmt::Debug) -> Self {
        Self::Database {
            details: format!("{source:?}"),
        }
    }
}

/// Multicall3 helper failure.
#[derive(Debug, thiserror::Error)]
pub enum MulticallError {
    /// Underlying EVM cache call failed.
    #[error(transparent)]
    Cache(#[from] CacheError),
    /// The aggregate3 call reverted or halted.
    #[error("Multicall3 aggregate call failed: {result}")]
    AggregateFailed {
        /// Debug rendering of the execution result.
        result: String,
    },
    /// A per-call result marked `success = false`.
    #[error("multicall result indicates the call failed")]
    CallFailed,
    /// ABI decode failure.
    #[error("failed to decode multicall result: {details}")]
    Decode {
        /// Decoder error text.
        details: String,
    },
}

/// Result type returned by Multicall3 helpers.
pub type MulticallResult<T> = Result<T, MulticallError>;

/// Deployment and Foundry-artifact loading failure.
#[derive(Debug, thiserror::Error)]
pub enum DeployError {
    /// Artifact file could not be read.
    #[error("failed to read Foundry artifact at {path}: {source}")]
    ReadArtifact {
        /// Artifact path.
        path: PathBuf,
        /// Filesystem error.
        #[source]
        source: io::Error,
    },
    /// Artifact JSON was invalid.
    #[error("failed to parse Foundry artifact JSON at {path}: {source}")]
    ParseArtifact {
        /// Artifact path.
        path: PathBuf,
        /// JSON parse error.
        #[source]
        source: serde_json::Error,
    },
    /// Artifact had no bytecode field.
    #[error("artifact {path} has no `bytecode` field")]
    MissingBytecodeField {
        /// Artifact path.
        path: PathBuf,
    },
    /// Artifact bytecode was not a supported string shape.
    #[error("artifact {path} has no `bytecode.object` string")]
    MissingBytecodeObject {
        /// Artifact path.
        path: PathBuf,
    },
    /// Bytecode was empty.
    #[error("empty bytecode")]
    EmptyBytecode,
    /// Bytecode still contains unresolved Foundry library placeholders.
    #[error("bytecode contains unresolved library placeholders")]
    UnresolvedLibraryPlaceholders,
    /// Hex bytecode could not be decoded.
    #[error("invalid hex bytecode: {details}")]
    InvalidHex {
        /// Hex decoder error text.
        details: String,
    },
    /// Underlying cache operation failed.
    #[error(transparent)]
    Cache(#[from] CacheError),
    /// Artifact deployment failed, with path context.
    #[error("deploying Foundry artifact {path} failed: {source}")]
    ArtifactDeploy {
        /// Artifact path.
        path: PathBuf,
        /// Cache operation failure.
        #[source]
        source: CacheError,
    },
    /// Target validation failed before etching.
    #[error("validating target contract {target} failed: {source}")]
    TargetValidation {
        /// Target address.
        target: Address,
        /// Cache operation failure.
        #[source]
        source: CacheError,
    },
    /// Runtime bytecode etch failed.
    #[error("etching runtime bytecode at {target} failed: {source}")]
    EtchRuntime {
        /// Target address.
        target: Address,
        /// Cache operation failure.
        #[source]
        source: CacheError,
    },
}

/// Result type returned by deployment helpers.
pub type DeployResult<T> = Result<T, DeployError>;

/// Access-list pricing query failure.
#[derive(Debug, thiserror::Error)]
pub enum AccessListError {
    /// Provider/oracle query failed.
    #[error("failed to query {operation}: {details}")]
    Query {
        /// Operation being queried.
        operation: &'static str,
        /// Provider/transport error text.
        details: String,
    },
}

impl AccessListError {
    /// Build a query error from any displayable provider error.
    pub fn query(operation: &'static str, source: impl fmt::Display) -> Self {
        Self::Query {
            operation,
            details: source.to_string(),
        }
    }
}

/// Result type returned by access-list pricing helpers.
pub type AccessListResult<T> = Result<T, AccessListError>;

/// Error returned by speculative freshness orchestration.
#[derive(Debug, thiserror::Error)]
pub enum FreshnessError {
    /// The validation handle was already consumed.
    #[error("validation handle already consumed")]
    ValidationHandleConsumed,
    /// The background validation task failed to join.
    #[error("validation task failed: {source}")]
    ValidationTaskFailed {
        /// Tokio join failure.
        #[source]
        source: tokio::task::JoinError,
    },
    /// An optimistic simulation failed before validation was spawned.
    #[error(transparent)]
    Overlay(#[from] OverlayError),
}

/// Result type returned by freshness APIs.
pub type FreshnessResult<T> = Result<T, FreshnessError>;

/// Result type returned by simulation entry points: `Ok(T)` on success, or a
/// [`SimError`] distinguishing a transaction-level revert, an EVM halt, and a
/// host-side failure.
pub type SimulationResult<T> = Result<T, SimError>;

/// Error returned by simulation entry points.
///
/// Distinguishes the three outcomes a caller must branch on: a transaction-level
/// [`Revert`](SimError::Revert) (with a decoded reason), an EVM
/// [`Halt`](SimError::Halt) (e.g. out of gas), and a host-side
/// [`Other`](SimError::Other) failure (RPC, database, ABI encoding).
///
/// Note that when a revert decodes to [`RevertReason::Panic`], panic codes
/// exceeding `u64::MAX` are dropped to `None` and so surface as
/// [`RevertReason::Unknown`] rather than `Panic`. This is benign: real
/// compiler-emitted panic codes are single-byte constants.
#[derive(Debug, thiserror::Error)]
pub enum SimError {
    /// The transaction reverted; carries the decoded revert.
    #[error("transaction reverted: {0}")]
    Revert(#[source] Box<SimulationError>),
    /// The EVM halted without returning revert data (e.g. out of gas, stack
    /// overflow). `reason` is the debug rendering of revm's halt reason.
    #[error("transaction halted: {reason} (gas used {gas_used})")]
    Halt {
        /// Debug rendering of the EVM halt reason.
        reason: String,
        /// Gas consumed before the halt.
        gas_used: u64,
    },
    /// An unexpected host-side error (RPC, database, ABI encoding).
    #[error("{0}")]
    Other(#[source] SimHostError),
}

impl SimError {
    /// `true` if this is a transaction-level revert, i.e. the
    /// [`Revert`](SimError::Revert) variant.
    pub fn is_revert(&self) -> bool {
        matches!(self, SimError::Revert(_))
    }

    /// `true` if the EVM halted without returning revert data (e.g. out of
    /// gas), i.e. the [`Halt`](SimError::Halt) variant.
    pub fn is_halt(&self) -> bool {
        matches!(self, SimError::Halt { .. })
    }

    /// The decoded [`SimulationError`] if this is a
    /// [`Revert`](SimError::Revert), or `None` for a
    /// [`Halt`](SimError::Halt) or [`Other`](SimError::Other) error.
    pub fn as_revert(&self) -> Option<&SimulationError> {
        match self {
            SimError::Revert(e) => Some(e),
            _ => None,
        }
    }
}

impl From<SimHostError> for SimError {
    fn from(e: SimHostError) -> Self {
        SimError::Other(e)
    }
}

impl From<CacheError> for SimError {
    fn from(e: CacheError) -> Self {
        SimError::Other(e.into())
    }
}

impl From<OverlayError> for SimError {
    fn from(e: OverlayError) -> Self {
        SimError::Other(e.into())
    }
}

impl From<SimulationError> for SimError {
    fn from(e: SimulationError) -> Self {
        SimError::Revert(Box::new(e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, U256};
    use alloy_sol_types::sol;

    sol! {
        #[derive(Debug)]
        error Unauthorized(address caller);
        #[derive(Debug)]
        error Paused();
        #[derive(Debug)]
        error ERC20InsufficientBalance(address sender, uint256 balance, uint256 needed);
    }

    /// Build ABI-encoded revert data for a standard `Error(string)` revert.
    /// Layout: selector(4) | offset(32) | length(32) | utf8 bytes (padded).
    fn encode_solidity_error(message: &str) -> Bytes {
        let bytes = message.as_bytes();
        let mut out = Vec::new();
        out.extend_from_slice(&ERROR_SELECTOR);

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
        let reason = decode_revert_reason(&data);
        assert_eq!(
            reason,
            RevertReason::Error("transfer amount exceeds balance".to_string())
        );

        let err = SimulationError::from_revert(21_000, data);
        assert_eq!(
            err.revert_message(),
            Some("transfer amount exceeds balance")
        );
        assert!(err.panic_code().is_none());
        assert!(err.custom_error().is_none());
    }

    #[test]
    fn decodes_panic_uint256() {
        // selector(4) | uint256 panic code (0x11 = arithmetic overflow).
        let mut data = PANIC_SELECTOR.to_vec();
        let mut code = [0u8; 32];
        code[31] = 0x11;
        data.extend_from_slice(&code);
        let data = Bytes::from(data);

        let reason = decode_revert_reason(&data);
        assert_eq!(reason, RevertReason::Panic(0x11));

        let err = SimulationError::from_revert(0, data);
        assert_eq!(err.panic_code(), Some(0x11));
        assert!(err.revert_message().is_none());
    }

    #[test]
    fn standard_decoder_does_not_recognize_custom_errors() {
        // A registered-only selector is Unknown to the standard decoder.
        let data = Bytes::from(Paused::SELECTOR.to_vec());
        match decode_revert_reason(&data) {
            RevertReason::Unknown { selector, .. } => {
                assert_eq!(selector.as_slice(), &Paused::SELECTOR);
            }
            other => panic!("expected Unknown, got {other}"),
        }
    }

    #[test]
    fn decodes_registered_custom_error_without_params() {
        let decoder = RevertDecoder::new().with_error::<Paused>();
        let data = Bytes::from(Paused::SELECTOR.to_vec());

        match decoder.decode(&data) {
            RevertReason::Custom(custom) => {
                assert_eq!(custom.name, "Paused()");
                assert_eq!(custom.selector.as_slice(), &Paused::SELECTOR);
                assert_eq!(custom.params.as_deref(), Some("Paused"));
            }
            other => panic!("expected Custom, got {other}"),
        }
    }

    #[test]
    fn decodes_registered_custom_error_with_params() {
        let decoder = RevertDecoder::new()
            .with_error::<Unauthorized>()
            .with_error::<ERC20InsufficientBalance>();

        let caller = Address::repeat_byte(0xAB);
        let data = Bytes::from(Unauthorized { caller }.abi_encode());
        let custom = match decoder.decode(&data) {
            RevertReason::Custom(custom) => custom,
            other => panic!("expected Custom, got {other}"),
        };
        assert_eq!(custom.name, "Unauthorized(address)");
        let params = custom.params.expect("params should decode");
        // The Debug rendering of the decoded struct includes the address.
        assert!(params.contains(&format!("{caller:?}")), "got {params}");

        // The IERC6093 standard error decodes through the same mechanism.
        let data = Bytes::from(
            ERC20InsufficientBalance {
                sender: caller,
                balance: U256::from(1u64),
                needed: U256::from(2u64),
            }
            .abi_encode(),
        );
        match decoder.decode(&data) {
            RevertReason::Custom(custom) => {
                assert_eq!(
                    custom.name,
                    "ERC20InsufficientBalance(address,uint256,uint256)"
                );
            }
            other => panic!("expected Custom, got {other}"),
        }
    }

    #[test]
    fn register_raw_decodes_by_selector() {
        let mut decoder = RevertDecoder::new();
        decoder.register_raw([0xde, 0xad, 0xbe, 0xef], "MyError(uint256)", |data| {
            Some(format!("raw {} bytes", data.len()))
        });

        let data = Bytes::from(vec![0xde, 0xad, 0xbe, 0xef, 0x00]);
        match decoder.decode(&data) {
            RevertReason::Custom(custom) => {
                assert_eq!(custom.name, "MyError(uint256)");
                assert_eq!(custom.params.as_deref(), Some("raw 5 bytes"));
            }
            other => panic!("expected Custom, got {other}"),
        }
    }

    #[test]
    fn unknown_blob_is_classified_as_unknown() {
        let data = Bytes::from(vec![0xde, 0xad, 0xbe, 0xef, 0x00, 0x01, 0x02, 0x03]);
        match decode_revert_reason(&data) {
            RevertReason::Unknown {
                selector,
                data: blob,
            } => {
                assert_eq!(selector.as_slice(), &[0xde, 0xad, 0xbe, 0xef]);
                assert_eq!(blob.len(), 8);
            }
            other => panic!("expected Unknown, got {other}"),
        }

        let err = SimulationError::from_revert(0, data);
        assert_eq!(
            err.selector().map(|s| s.to_vec()),
            Some(vec![0xde, 0xad, 0xbe, 0xef])
        );
        assert!(!err.is_empty_revert());
    }

    #[test]
    fn empty_revert_data_decodes_to_empty() {
        let reason = decode_revert_reason(&Bytes::new());
        assert_eq!(reason, RevertReason::Empty);

        let err = SimulationError::from_revert(0, Bytes::new());
        assert!(err.is_empty_revert());
        assert!(err.selector().is_none());
    }

    #[test]
    fn data_shorter_than_selector_is_unknown_with_padded_selector() {
        let data = Bytes::from(vec![0x01, 0x02, 0x03]);
        match decode_revert_reason(&data) {
            RevertReason::Unknown { selector, .. } => {
                assert_eq!(selector.as_slice(), &[0x01, 0x02, 0x03, 0x00]);
            }
            other => panic!("expected Unknown, got {other}"),
        }
    }
}
