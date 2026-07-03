//! Call-frame tracing and a composing inspector seam.
//!
//! This module provides [`CallTracer`], a [`revm::Inspector`] that reconstructs
//! the **call-frame tree** of a simulation — the top-level call plus every nested
//! `CALL`/`STATICCALL`/`DELEGATECALL`/`CALLCODE` and `CREATE`/`CREATE2` frame —
//! without opcode/step-level tracing. Each [`CallTrace`] records the caller, the
//! callee (or created address), the call value, calldata, gas used, return data,
//! a [`CallStatus`], the call depth, and its child frames.
//!
//! It also provides [`InspectorStack`], a tiny composing inspector that fans out
//! every [`Inspector`] hook to two inner inspectors so, e.g., a `CallTracer` and a
//! [`TransferInspector`](crate::inspector::TransferInspector) can run in a single
//! pass and each produce its own independent result.
//!
//! Attach either via the inspector-generic
//! [`EvmOverlay::call_raw_with_inspector`](crate::cache::EvmOverlay::call_raw_with_inspector).
//!
//! # Calldata resolution caveat
//!
//! revm represents a frame's calldata as a [`CallInput`], which is either owned
//! [`CallInput::Bytes`] or a [`CallInput::SharedBuffer`] range into the EVM's
//! shared-memory scratch buffer. Resolving a `SharedBuffer` range back to bytes
//! requires the concrete EVM context (`ContextTr`), which this inspector — written
//! against the same fully-generic `CTX` as the existing
//! [`TransferInspector`](crate::inspector::TransferInspector) — deliberately does
//! not bind. The **top-level** call's calldata is always `CallInput::Bytes` (revm
//! builds it directly from the transaction), so a root frame's
//! [`input`](CallTrace::input) is always the real calldata. Nested calls whose
//! calldata is a `SharedBuffer` are recorded with an **empty** `input`; their
//! callee address, value, gas, status, and subcalls are captured faithfully. This
//! is a documented limitation, not a correctness bug: the tracer never fabricates
//! calldata it cannot resolve.

use alloy_primitives::{Address, Bytes, Log, U256};
use revm::Inspector;
use revm::interpreter::{
    CallInput, CallInputs, CallOutcome, CallScheme, CreateInputs, CreateOutcome, CreateScheme,
    Interpreter, InterpreterTypes,
};

/// The kind of EVM frame a [`CallTrace`] represents.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CallKind {
    /// A `CALL`.
    Call,
    /// A `STATICCALL`.
    StaticCall,
    /// A `DELEGATECALL`.
    DelegateCall,
    /// A `CALLCODE`.
    CallCode,
    /// A `CREATE`.
    Create,
    /// A `CREATE2`.
    Create2,
}

impl CallKind {
    /// Map a revm [`CallScheme`] (the opcode behind a message call) to a [`CallKind`].
    fn from_call_scheme(scheme: CallScheme) -> Self {
        match scheme {
            CallScheme::Call => CallKind::Call,
            CallScheme::CallCode => CallKind::CallCode,
            CallScheme::DelegateCall => CallKind::DelegateCall,
            CallScheme::StaticCall => CallKind::StaticCall,
        }
    }

    /// Map a revm [`CreateScheme`] to a [`CallKind`].
    ///
    /// `CreateScheme::Custom` (an internally-addressed create) is reported as
    /// [`CallKind::Create`].
    fn from_create_scheme(scheme: CreateScheme) -> Self {
        match scheme {
            CreateScheme::Create => CallKind::Create,
            CreateScheme::Create2 { .. } => CallKind::Create2,
            CreateScheme::Custom { .. } => CallKind::Create,
        }
    }
}

/// The terminal status of an EVM frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CallStatus {
    /// The frame returned normally (`STOP`/`RETURN`/`SELFDESTRUCT`).
    Success,
    /// The frame reverted (`REVERT`, or a revert-class condition).
    Revert,
    /// The frame halted on an exceptional error (e.g. out of gas, invalid opcode).
    Halt,
}

/// A single node in the call-frame tree captured by a [`CallTracer`].
///
/// One `CallTrace` is produced per EVM message call or contract creation. Child
/// frames (the calls/creates made *by* this frame) are nested under
/// [`subcalls`](Self::subcalls) in execution order.
#[derive(Clone, Debug)]
pub struct CallTrace {
    /// Whether this frame is a call (and which kind) or a contract creation.
    pub kind: CallKind,
    /// The caller (the account initiating this frame).
    pub from: Address,
    /// The callee, or — for a `CREATE`/`CREATE2` — the created contract address.
    pub to: Address,
    /// The call value (wei). Always zero for `STATICCALL`.
    pub value: U256,
    /// The calldata (for a create, the init code). Empty when the calldata was a
    /// shared-memory range this tracer could not resolve — see the
    /// [module docs](crate::tracing#calldata-resolution-caveat).
    pub input: Bytes,
    /// Gas spent executing this frame.
    pub gas_used: u64,
    /// The frame's return data (for a successful create, the deployed code).
    pub output: Bytes,
    /// The terminal status of the frame.
    pub status: CallStatus,
    /// The frame's call depth (the top-level call is depth `0`).
    pub depth: usize,
    /// Child frames (calls/creates made by this frame), in execution order.
    pub subcalls: Vec<CallTrace>,
}

/// A frame whose `*_end` hook has not yet fired (its output/gas/status are unset).
///
/// While a frame is open its already-completed children accumulate in `subcalls`;
/// when the matching `call_end`/`create_end` fires it is finalized into a
/// [`CallTrace`] and attached to its parent (or installed as the root).
#[derive(Clone, Debug)]
struct PendingFrame {
    kind: CallKind,
    from: Address,
    to: Address,
    value: U256,
    input: Bytes,
    depth: usize,
    subcalls: Vec<CallTrace>,
}

/// A [`revm::Inspector`] that builds a [`CallTrace`] tree from the call/create
/// frame hooks.
///
/// Drive it through
/// [`EvmOverlay::call_raw_with_inspector`](crate::cache::EvmOverlay::call_raw_with_inspector),
/// then read the captured tree with [`root`](Self::root) or [`into_trace`](Self::into_trace).
///
/// Only the call-frame hooks (`call`/`call_end`/`create`/`create_end`) are used;
/// no opcode/step, `SLOAD`/`SSTORE`, or per-opcode gas tracing is performed.
///
/// ```
/// use evm_fork_cache::CallTracer;
///
/// let tracer = CallTracer::new();
/// assert!(tracer.root().is_none()); // nothing executed yet
/// ```
#[derive(Clone, Debug, Default)]
pub struct CallTracer {
    /// Open frames, innermost last. A `call`/`create` pushes; the matching
    /// `*_end` pops.
    stack: Vec<PendingFrame>,
    /// The finalized top-level frame, set when the outermost frame's `*_end` fires.
    root: Option<CallTrace>,
}

impl CallTracer {
    /// Create an empty tracer with no captured frames.
    pub fn new() -> Self {
        Self::default()
    }

    /// The root (top-level) frame after a transact, or `None` if nothing executed.
    pub fn root(&self) -> Option<&CallTrace> {
        self.root.as_ref()
    }

    /// Consume the tracer and return the root frame, or `None` if nothing executed.
    pub fn into_trace(self) -> Option<CallTrace> {
        self.root
    }

    /// Push a new open frame onto the stack at the current depth.
    fn push_frame(
        &mut self,
        kind: CallKind,
        from: Address,
        to: Address,
        value: U256,
        input: Bytes,
    ) {
        let depth = self.stack.len();
        self.stack.push(PendingFrame {
            kind,
            from,
            to,
            value,
            input,
            depth,
            subcalls: Vec::new(),
        });
    }

    /// Finalize the innermost open frame into a [`CallTrace`] and attach it to its
    /// parent (or install it as the root if it was the top-level frame).
    ///
    /// `to_override` lets the create hooks supply the created address, which is
    /// not known until `create_end`.
    fn pop_frame(
        &mut self,
        gas_used: u64,
        output: Bytes,
        status: CallStatus,
        to_override: Option<Address>,
    ) {
        let Some(pending) = self.stack.pop() else {
            // Defensive: an unbalanced `*_end` with no matching open frame. revm
            // pairs the hooks, so this should not happen; ignore rather than panic.
            return;
        };

        let trace = CallTrace {
            kind: pending.kind,
            from: pending.from,
            to: to_override.unwrap_or(pending.to),
            value: pending.value,
            input: pending.input,
            gas_used,
            output,
            status,
            depth: pending.depth,
            subcalls: pending.subcalls,
        };

        if let Some(parent) = self.stack.last_mut() {
            parent.subcalls.push(trace);
        } else {
            self.root = Some(trace);
        }
    }
}

/// Resolve a frame's [`CallInput`] to owned bytes.
///
/// Owned [`CallInput::Bytes`] are cloned directly. A [`CallInput::SharedBuffer`]
/// range cannot be resolved without the concrete EVM context, so it yields empty
/// bytes — see the [module docs](crate::tracing#calldata-resolution-caveat).
fn resolve_call_input(input: &CallInput) -> Bytes {
    match input {
        CallInput::Bytes(bytes) => bytes.clone(),
        CallInput::SharedBuffer(_) => Bytes::new(),
    }
}

/// Maps a frame's terminal [`InstructionResult`](revm::interpreter::InstructionResult)
/// to a [`CallStatus`].
fn status_from_result(result: revm::interpreter::InstructionResult) -> CallStatus {
    if result.is_ok() {
        CallStatus::Success
    } else if result.is_revert() {
        CallStatus::Revert
    } else {
        // Everything else is an exceptional halt (out of gas, invalid opcode, …).
        CallStatus::Halt
    }
}

impl<CTX, INTR> Inspector<CTX, INTR> for CallTracer
where
    INTR: InterpreterTypes,
{
    fn call(&mut self, _context: &mut CTX, inputs: &mut CallInputs) -> Option<CallOutcome> {
        self.push_frame(
            CallKind::from_call_scheme(inputs.scheme),
            inputs.caller,
            inputs.target_address,
            inputs.call_value(),
            resolve_call_input(&inputs.input),
        );
        None
    }

    fn call_end(&mut self, _context: &mut CTX, _inputs: &CallInputs, outcome: &mut CallOutcome) {
        let status = status_from_result(*outcome.instruction_result());
        let gas_used = outcome.gas().spent();
        let output = outcome.output().clone();
        self.pop_frame(gas_used, output, status, None);
    }

    fn create(&mut self, _context: &mut CTX, inputs: &mut CreateInputs) -> Option<CreateOutcome> {
        self.push_frame(
            CallKind::from_create_scheme(inputs.scheme()),
            inputs.caller(),
            // The created address is not known until `create_end`; fill a
            // placeholder now and override it on finalize.
            Address::ZERO,
            inputs.value(),
            inputs.init_code().clone(),
        );
        None
    }

    fn create_end(
        &mut self,
        _context: &mut CTX,
        _inputs: &CreateInputs,
        outcome: &mut CreateOutcome,
    ) {
        let status = status_from_result(*outcome.instruction_result());
        let gas_used = outcome.gas().spent();
        let output = outcome.output().clone();
        self.pop_frame(gas_used, output, status, outcome.address);
    }
}

/// Runs two [`Inspector`]s over the same execution.
///
/// Every [`Inspector`] hook is fanned out to both inner inspectors (`.0` first,
/// then `.1`) so each captures its own result independently in a single pass. The
/// canonical use is pairing a [`CallTracer`] with a
/// [`TransferInspector`](crate::inspector::TransferInspector) to obtain both a
/// call-frame trace and the ERC-20 transfers from one simulation:
///
/// ```no_run
/// # use std::sync::Arc;
/// # use alloy_primitives::{Address, Bytes};
/// # use evm_fork_cache::cache::{EvmOverlay, EvmSnapshot, TxConfig};
/// # use evm_fork_cache::inspector::TransferInspector;
/// # use evm_fork_cache::{CallTracer, InspectorStack};
/// # fn run(snapshot: Arc<EvmSnapshot>, from: Address, to: Address) -> Result<(), Box<dyn std::error::Error>> {
/// let mut overlay = EvmOverlay::new(snapshot, None);
/// let (_result, stack) = overlay.call_raw_with_inspector(
///     from,
///     to,
///     Bytes::new(),
///     &TxConfig::default(),
///     InspectorStack(CallTracer::new(), TransferInspector::new()),
///     false,
/// )?;
/// let InspectorStack(tracer, transfer) = stack;
/// let _trace = tracer.into_trace();
/// let _transfers = transfer.transfers;
/// # Ok(())
/// # }
/// ```
///
/// For the `call`/`create` hooks — which return `Option<…Outcome>` to optionally
/// *override* the result — the first inspector that returns `Some` wins and the
/// second's hook is not called, mirroring revm's own composition of inspector
/// tuples. The observe-only inspectors here ([`CallTracer`],
/// [`TransferInspector`](crate::inspector::TransferInspector)) always return
/// `None`, so both always observe.
#[derive(Clone, Debug, Default)]
pub struct InspectorStack<A, B>(pub A, pub B);

impl<A, B, CTX, INTR> Inspector<CTX, INTR> for InspectorStack<A, B>
where
    INTR: InterpreterTypes,
    A: Inspector<CTX, INTR>,
    B: Inspector<CTX, INTR>,
{
    fn initialize_interp(&mut self, interp: &mut Interpreter<INTR>, context: &mut CTX) {
        self.0.initialize_interp(interp, context);
        self.1.initialize_interp(interp, context);
    }

    fn step(&mut self, interp: &mut Interpreter<INTR>, context: &mut CTX) {
        self.0.step(interp, context);
        self.1.step(interp, context);
    }

    fn step_end(&mut self, interp: &mut Interpreter<INTR>, context: &mut CTX) {
        self.0.step_end(interp, context);
        self.1.step_end(interp, context);
    }

    fn log(&mut self, context: &mut CTX, log: Log) {
        self.0.log(context, log.clone());
        self.1.log(context, log);
    }

    fn log_full(&mut self, interp: &mut Interpreter<INTR>, context: &mut CTX, log: Log) {
        self.0.log_full(interp, context, log.clone());
        self.1.log_full(interp, context, log);
    }

    fn call(&mut self, context: &mut CTX, inputs: &mut CallInputs) -> Option<CallOutcome> {
        self.0
            .call(context, inputs)
            .or_else(|| self.1.call(context, inputs))
    }

    fn call_end(&mut self, context: &mut CTX, inputs: &CallInputs, outcome: &mut CallOutcome) {
        self.0.call_end(context, inputs, outcome);
        self.1.call_end(context, inputs, outcome);
    }

    fn create(&mut self, context: &mut CTX, inputs: &mut CreateInputs) -> Option<CreateOutcome> {
        self.0
            .create(context, inputs)
            .or_else(|| self.1.create(context, inputs))
    }

    fn create_end(
        &mut self,
        context: &mut CTX,
        inputs: &CreateInputs,
        outcome: &mut CreateOutcome,
    ) {
        self.0.create_end(context, inputs, outcome);
        self.1.create_end(context, inputs, outcome);
    }

    fn selfdestruct(&mut self, contract: Address, target: Address, value: U256) {
        self.0.selfdestruct(contract, target, value);
        self.1.selfdestruct(contract, target, value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_tracer_has_no_root() {
        let tracer = CallTracer::new();
        assert!(tracer.root().is_none());
        assert!(tracer.into_trace().is_none());
    }

    #[test]
    fn call_kind_maps_call_schemes() {
        assert_eq!(CallKind::from_call_scheme(CallScheme::Call), CallKind::Call);
        assert_eq!(
            CallKind::from_call_scheme(CallScheme::StaticCall),
            CallKind::StaticCall
        );
        assert_eq!(
            CallKind::from_call_scheme(CallScheme::DelegateCall),
            CallKind::DelegateCall
        );
        assert_eq!(
            CallKind::from_call_scheme(CallScheme::CallCode),
            CallKind::CallCode
        );
    }

    #[test]
    fn call_kind_maps_create_schemes() {
        assert_eq!(
            CallKind::from_create_scheme(CreateScheme::Create),
            CallKind::Create
        );
        assert_eq!(
            CallKind::from_create_scheme(CreateScheme::Create2 { salt: U256::ZERO }),
            CallKind::Create2
        );
    }

    #[test]
    fn resolve_input_reads_owned_bytes_and_empties_shared_buffer() {
        let owned = CallInput::Bytes(Bytes::from(vec![1, 2, 3]));
        assert_eq!(resolve_call_input(&owned), Bytes::from(vec![1, 2, 3]));

        let shared = CallInput::SharedBuffer(0..8);
        assert!(resolve_call_input(&shared).is_empty());
    }

    #[test]
    fn status_mapping_classifies_results() {
        use revm::interpreter::InstructionResult;
        assert_eq!(
            status_from_result(InstructionResult::Stop),
            CallStatus::Success
        );
        assert_eq!(
            status_from_result(InstructionResult::Return),
            CallStatus::Success
        );
        assert_eq!(
            status_from_result(InstructionResult::Revert),
            CallStatus::Revert
        );
        assert_eq!(
            status_from_result(InstructionResult::OutOfGas),
            CallStatus::Halt
        );
    }

    /// A finalized child frame attaches to its open parent, and the parent
    /// becomes the root when its own `*_end` fires.
    #[test]
    fn frames_nest_into_a_tree() {
        let mut tracer = CallTracer::new();
        let root_addr = Address::repeat_byte(0x11);
        let child_addr = Address::repeat_byte(0x22);

        tracer.push_frame(
            CallKind::Call,
            Address::ZERO,
            root_addr,
            U256::ZERO,
            Bytes::from(vec![0xaa]),
        );
        tracer.push_frame(
            CallKind::StaticCall,
            root_addr,
            child_addr,
            U256::ZERO,
            Bytes::new(),
        );
        // Inner frame ends first.
        tracer.pop_frame(10, Bytes::new(), CallStatus::Success, None);
        // Outer frame ends, becoming the root.
        tracer.pop_frame(100, Bytes::from(vec![0xbb]), CallStatus::Success, None);

        let root = tracer.into_trace().expect("root frame");
        assert_eq!(root.to, root_addr);
        assert_eq!(root.depth, 0);
        assert_eq!(root.input, Bytes::from(vec![0xaa]));
        assert_eq!(root.subcalls.len(), 1);
        assert_eq!(root.subcalls[0].to, child_addr);
        assert_eq!(root.subcalls[0].depth, 1);
        assert_eq!(root.subcalls[0].kind, CallKind::StaticCall);
    }
}
