use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use alloy_eips::eip2930::{AccessList, AccessListItem};
use alloy_primitives::{Address, B256, Bytes, TxKind, U256};
use anyhow::{Result, anyhow};
use foundry_fork_db::{DatabaseError, SharedBackend};
use revm::{
    Context, ExecuteCommitEvm, ExecuteEvm, InspectEvm, MainBuilder, MainContext,
    context::{BlockEnv, CfgEnv, Journal, LocalContext, TxEnv, result::ExecutionResult},
    database_interface::{Database, DatabaseRef},
    state::{AccountInfo, Bytecode},
};

use super::snapshot::EvmSnapshot;
use super::{CallSimulationResult, SimStatus, TxConfig, unix_timestamp_secs_saturating};
use crate::access_set::StorageAccessList;
use crate::bundle::{BundleOptions, BundleResult, BundleTx, RevertPolicy, TxOutcome};
use crate::errors::{SimError, SimulationError, SimulationResult};
use crate::inspector::TransferInspector;

type OverlayEvm<'a> = revm::MainnetEvm<
    Context<BlockEnv, TxEnv, CfgEnv, &'a mut EvmOverlay, Journal<&'a mut EvmOverlay>, ()>,
>;

type InspectorOverlayEvm<'a, INSP> = revm::MainnetEvm<
    Context<BlockEnv, TxEnv, CfgEnv, &'a mut EvmOverlay, Journal<&'a mut EvmOverlay>, ()>,
    INSP,
>;

/// Per-simulation mutable overlay on an immutable snapshot.
///
/// Lookup order: dirty layer → snapshot → ext_db (optional RPC fallback).
///
/// This type is `Send` (unlike `EvmCache`) because it uses no `Rc`/`RefCell`.
/// Each simulation task gets its own `EvmOverlay` with a cheap `Arc::clone`
/// of the shared `EvmSnapshot`.
///
/// # Reuse across simulations (Pillar A.2)
///
/// A worker doing many sims against the same snapshot can call [`Self::new`]
/// once and [`Self::reset`] between sims instead of allocating a fresh overlay
/// each time. The reusable shared-memory buffer is also recycled across calls —
/// see [`Self::call_raw`] — without making the overlay `!Send`.
pub struct EvmOverlay {
    snapshot: Arc<EvmSnapshot>,
    /// Per-simulation mutations (accounts fetched from ext_db, committed changes).
    dirty_accounts: HashMap<Address, AccountInfo>,
    /// Per-simulation storage mutations.
    dirty_storage: HashMap<Address, HashMap<U256, U256>>,
    /// Optional RPC fallback for data not in snapshot.
    ext_db: Option<SharedBackend>,
    /// Reusable shared-memory buffer, recycled across the build→transact→revert
    /// call methods to avoid reallocating a 64 KB `Vec` per call.
    ///
    /// Stored as a plain `Vec<u8>` (not an `Rc`) so the overlay stays `Send`. A
    /// call method `mem::take`s it, wraps it in a method-local `Rc<RefCell<_>>`
    /// for revm's [`LocalContext`], runs, then reclaims and clears it after the
    /// EVM is dropped (see [`Self::build_evm_with_local`]).
    reusable_buffer: Vec<u8>,
    /// Target pre-allocation (bytes) for [`Self::reusable_buffer`] and each
    /// per-call buffer, taken from the snapshot's configured
    /// [`SharedMemoryCapacity`](super::SharedMemoryCapacity) so overlays honor the
    /// capacity set on the originating [`EvmCache`].
    buffer_capacity: usize,
}

impl EvmOverlay {
    /// Create a new overlay on the given snapshot.
    ///
    /// The reusable shared-memory buffer is pre-allocated to the snapshot's
    /// configured shared-memory capacity (see
    /// [`SharedMemoryCapacity`](super::SharedMemoryCapacity)).
    pub fn new(snapshot: Arc<EvmSnapshot>, ext_db: Option<SharedBackend>) -> Self {
        let buffer_capacity = snapshot.shared_memory_capacity;
        Self {
            snapshot,
            dirty_accounts: HashMap::new(),
            dirty_storage: HashMap::new(),
            ext_db,
            reusable_buffer: Vec::with_capacity(buffer_capacity),
            buffer_capacity,
        }
    }

    /// Clear the per-simulation dirty layer so this overlay can be reused for the
    /// next simulation against the same snapshot, without reallocating (Pillar
    /// A.2).
    ///
    /// A worker doing K sims calls [`Self::new`] once and `reset()` between sims
    /// instead of allocating a fresh overlay (plus dirty maps plus an `Arc`
    /// clone) each time. After `reset()` the overlay reads the pristine snapshot
    /// again — it is exactly equivalent to a freshly-built overlay on the same
    /// snapshot. The snapshot `Arc`, the optional `ext_db`, and the reusable
    /// shared-memory buffer (kept at capacity) are retained.
    pub fn reset(&mut self) {
        self.dirty_accounts.clear();
        self.dirty_storage.clear();
        // Keep: snapshot Arc, ext_db, and the reusable buffer. The buffer is
        // already cleared after each call, so nothing to do for it here.
    }

    /// Chain ID of the block context captured by the underlying snapshot.
    ///
    /// This is the value installed into `cfg.chain_id` by [`Self::build_evm`].
    pub fn chain_id(&self) -> u64 {
        self.snapshot.chain_id
    }

    /// Block number of the snapshot's block context, or `None` if it was not
    /// captured.
    ///
    /// When present this is the `block.number` simulations run against; when
    /// `None`, [`Self::build_evm`] leaves revm's default block number in place.
    pub fn block_number(&self) -> Option<u64> {
        self.snapshot.block_number
    }

    /// Base fee of the snapshot's block context, or `None` if it was not
    /// captured.
    ///
    /// Note that base-fee checks are disabled in the simulation EVM, so this is
    /// informational rather than enforced against the transaction.
    pub fn basefee(&self) -> Option<u64> {
        self.snapshot.basefee
    }

    /// Timestamp of the snapshot's block context, or `None` if it was not
    /// captured.
    ///
    /// When `None`, [`Self::build_evm`] substitutes the current wall-clock time
    /// for `block.timestamp`.
    pub fn timestamp(&self) -> Option<u64> {
        self.snapshot.timestamp
    }

    /// A fresh [`LocalContext`] with a newly-allocated 64 KB shared-memory buffer.
    ///
    /// Used by the public [`Self::build_evm`], which hands out the EVM and cannot
    /// reclaim its buffer afterwards. The internal call methods instead recycle
    /// [`Self::reusable_buffer`] via [`Self::build_evm_with_local`].
    fn fresh_local(&self) -> LocalContext {
        LocalContext {
            shared_memory_buffer: Rc::new(RefCell::new(Vec::with_capacity(self.buffer_capacity))),
            precompile_error_message: None,
        }
    }

    /// Build a revm EVM instance backed by this overlay, using a caller-supplied
    /// [`LocalContext`].
    ///
    /// This is the shared body behind [`Self::build_evm`] and the internal call
    /// methods. The call methods pass a `local` wrapping the recycled
    /// [`Self::reusable_buffer`] (Pillar A.2) and reclaim it after the EVM is
    /// dropped; [`Self::build_evm`] passes a fresh one.
    ///
    /// Note: the returned EVM is `!Send` (due to `LocalContext`'s `Rc<RefCell>`),
    /// but this is fine because it's created and used within a single task.
    fn build_evm_with_local(&mut self, local: LocalContext) -> OverlayEvm<'_> {
        // Read snapshot values before the mutable borrow of self
        let chain_id = self.snapshot.chain_id;
        let spec_id = self.snapshot.spec_id;
        let timestamp = self
            .snapshot
            .timestamp
            .unwrap_or_else(|| unix_timestamp_secs_saturating(std::time::SystemTime::now()));
        let block_number = self.snapshot.block_number;
        let basefee = self.snapshot.basefee;
        let coinbase = self.snapshot.coinbase;
        let prevrandao = self.snapshot.prevrandao;
        let gas_limit = self.snapshot.gas_limit;

        let mut evm = Context::mainnet()
            .with_db(&mut *self)
            .with_local(local)
            .modify_cfg_chained(|cfg| {
                cfg.disable_nonce_check = true;
                cfg.disable_eip3607 = true;
                cfg.disable_base_fee = true;
                cfg.disable_balance_check = true;
                cfg.chain_id = chain_id;
                cfg.limit_contract_code_size = None;
                cfg.tx_chain_id_check = false;
                cfg.spec = spec_id;
            })
            .build_mainnet();

        evm.block.timestamp = U256::from(timestamp);
        if let Some(number) = block_number {
            evm.block.number = U256::from(number);
        }
        if let Some(basefee) = basefee {
            evm.block.basefee = basefee;
        }
        if let Some(coinbase) = coinbase {
            evm.block.beneficiary = coinbase;
        }
        if let Some(prevrandao) = prevrandao {
            evm.block.prevrandao = Some(prevrandao);
        }
        if let Some(gas_limit) = gas_limit {
            evm.block.gas_limit = gas_limit;
        }
        evm
    }

    /// Build a revm EVM instance backed by this overlay.
    ///
    /// This allocates a fresh 64 KB shared-memory buffer each call: it hands the
    /// EVM out to the caller and cannot reclaim the buffer afterwards, so it
    /// cannot recycle the overlay's reusable buffer. The internal call methods
    /// ([`Self::call_raw`], etc.) recycle the buffer instead (Pillar A.2).
    ///
    /// Note: The returned EVM is `!Send` (due to `LocalContext`'s `Rc<RefCell>`),
    /// but this is fine because it's created and used within a single task.
    pub fn build_evm(&mut self) -> OverlayEvm<'_> {
        let local = self.fresh_local();
        self.build_evm_with_local(local)
    }

    /// Execute a non-committing call and return the raw [`ExecutionResult`].
    ///
    /// The EVM state is reverted to a checkpoint after execution on *both*
    /// success and failure, so the call never mutates this overlay's dirty
    /// layer. Each overlay simulation is therefore isolated: repeated calls all
    /// observe the same base state.
    ///
    /// A revert or halt is *not* an error here — it is reported through the
    /// returned [`ExecutionResult`] variant. Only failure to build or transact
    /// the call yields `Err`.
    ///
    /// # Errors
    ///
    /// Returns an error if the [`TxEnv`] cannot be built from the given inputs,
    /// or if revm fails to transact the call (for example a database error
    /// while loading state from the RPC fallback).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use std::sync::Arc;
    /// # use alloy_primitives::{Address, Bytes};
    /// # use evm_fork_cache::cache::{EvmOverlay, EvmSnapshot};
    /// # fn run(snapshot: Arc<EvmSnapshot>) -> anyhow::Result<()> {
    /// let mut overlay = EvmOverlay::new(snapshot, None);
    /// let result = overlay.call_raw(Address::ZERO, Address::ZERO, Bytes::new())?;
    /// // State is reverted; a second call sees the same base state.
    /// let _again = overlay.call_raw(Address::ZERO, Address::ZERO, Bytes::new())?;
    /// # let _ = result;
    /// # Ok(())
    /// # }
    /// ```
    pub fn call_raw(
        &mut self,
        from: Address,
        to: Address,
        calldata: Bytes,
    ) -> Result<ExecutionResult> {
        let tx = TxEnv::builder()
            .caller(from)
            .kind(TxKind::Call(to))
            .data(calldata)
            .value(U256::ZERO)
            .build()
            .map_err(|e| anyhow!("Failed to build tx env: {:?}", e))?;

        // Recycle the reusable buffer (Pillar A.2): take it out as a plain Vec
        // (keeping the overlay Send), lend it to a method-local Rc<RefCell> for
        // revm's LocalContext, then reclaim and clear it after the EVM is dropped.
        let buffer = Rc::new(RefCell::new(std::mem::take(&mut self.reusable_buffer)));
        let local = LocalContext {
            shared_memory_buffer: Rc::clone(&buffer),
            precompile_error_message: None,
        };

        let result = {
            let mut evm = self.build_evm_with_local(local);
            use revm::context_interface::JournalTr;
            let checkpoint = evm.journaled_state.checkpoint();
            let result = evm
                .transact_one(tx)
                .map_err(|e| anyhow!("Failed to transact: {:?}", e));
            evm.journaled_state.checkpoint_revert(checkpoint);
            result
        };

        self.reclaim_buffer(buffer);
        result
    }

    /// Reclaim the recycled shared-memory buffer after the EVM (and its
    /// `LocalContext` clone of the `Rc`) has been dropped, clearing it for the
    /// next call.
    ///
    /// The `Rc` was only ever held by the dropped EVM and this method's local, so
    /// `try_unwrap` succeeds in the normal path. If a panic somewhere left an
    /// extra strong reference the buffer is simply re-allocated next call — no
    /// correctness impact.
    fn reclaim_buffer(&mut self, buffer: Rc<RefCell<Vec<u8>>>) {
        if let Ok(cell) = Rc::try_unwrap(buffer) {
            let mut buf = cell.into_inner();
            buf.clear();
            self.reusable_buffer = buf;
        } else {
            self.reusable_buffer = Vec::with_capacity(self.buffer_capacity);
        }
    }

    /// Build a revm EVM instance with an inspector, backed by this overlay, using
    /// a caller-supplied [`LocalContext`].
    ///
    /// Like [`Self::build_evm_with_local`] but attaches `inspector`. The call
    /// methods pass a `local` wrapping the recycled [`Self::reusable_buffer`]
    /// (Pillar A.2) and reclaim it after the EVM is dropped.
    fn build_evm_with_inspector_local<INSP>(
        &mut self,
        inspector: INSP,
        local: LocalContext,
    ) -> InspectorOverlayEvm<'_, INSP> {
        let chain_id = self.snapshot.chain_id;
        let spec_id = self.snapshot.spec_id;
        let timestamp = self
            .snapshot
            .timestamp
            .unwrap_or_else(|| unix_timestamp_secs_saturating(std::time::SystemTime::now()));
        let block_number = self.snapshot.block_number;
        let basefee = self.snapshot.basefee;
        let coinbase = self.snapshot.coinbase;
        let prevrandao = self.snapshot.prevrandao;
        let gas_limit = self.snapshot.gas_limit;

        let mut evm = Context::mainnet()
            .with_db(&mut *self)
            .with_local(local)
            .modify_cfg_chained(|cfg| {
                cfg.disable_nonce_check = true;
                cfg.disable_eip3607 = true;
                cfg.disable_base_fee = true;
                cfg.disable_balance_check = true;
                cfg.chain_id = chain_id;
                cfg.limit_contract_code_size = None;
                cfg.tx_chain_id_check = false;
                cfg.spec = spec_id;
            })
            .build_mainnet_with_inspector(inspector);

        evm.block.timestamp = U256::from(timestamp);
        if let Some(number) = block_number {
            evm.block.number = U256::from(number);
        }
        if let Some(basefee) = basefee {
            evm.block.basefee = basefee;
        }
        if let Some(coinbase) = coinbase {
            evm.block.beneficiary = coinbase;
        }
        if let Some(prevrandao) = prevrandao {
            evm.block.prevrandao = Some(prevrandao);
        }
        if let Some(gas_limit) = gas_limit {
            evm.block.gas_limit = gas_limit;
        }
        evm
    }

    /// Simulate a call with transfer tracking via the `TransferInspector`.
    ///
    /// This is the overlay-compatible equivalent of
    /// [`super::EvmCache::simulate_with_transfer_tracking`]. It captures ERC20
    /// Transfer events during execution to compute balance deltas for `owner`
    /// (restricted to `tokens` when provided) without relying on pre/post
    /// balance queries.
    ///
    /// On a reverting or halting call the EVM state is reverted to a checkpoint
    /// before returning, so a failed simulation never mutates this overlay. On
    /// success the call either commits the journaled changes into the overlay's
    /// dirty layer (`commit == true`) or reverts them (`commit == false`); a
    /// non-committing run leaves each overlay simulation isolated from the next.
    ///
    /// # Errors
    ///
    /// Returns an error if the [`TxEnv`] cannot be built, if revm fails to
    /// transact the call, if the call reverts (mapped from the revert payload),
    /// or if the call halts. In every error case the EVM state is reverted
    /// first, regardless of `commit`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use std::sync::Arc;
    /// # use alloy_primitives::{Address, Bytes};
    /// # use evm_fork_cache::cache::{EvmOverlay, EvmSnapshot};
    /// # fn run(snapshot: Arc<EvmSnapshot>, token: Address, owner: Address) -> anyhow::Result<()> {
    /// let mut overlay = EvmOverlay::new(snapshot, None);
    /// let sim = overlay.simulate_with_transfer_tracking(
    ///     owner,
    ///     token,
    ///     Bytes::new(),
    ///     owner,
    ///     Some([token]),
    ///     false, // non-committing: state is reverted afterwards
    /// )?;
    /// let _delta = sim.token_deltas.get(&token);
    /// # Ok(())
    /// # }
    /// ```
    pub fn simulate_with_transfer_tracking(
        &mut self,
        from: Address,
        to: Address,
        calldata: Bytes,
        owner: Address,
        tokens: Option<impl IntoIterator<Item = Address>>,
        commit: bool,
    ) -> SimulationResult<CallSimulationResult> {
        let tx = TxEnv::builder()
            .caller(from)
            .kind(TxKind::Call(to))
            .data(calldata)
            .value(U256::ZERO)
            .build()
            .map_err(|e| SimError::Other(anyhow!("Failed to build tx env: {:?}", e)))?;

        let inspector = TransferInspector::new();

        // Recycle the reusable buffer (Pillar A.2); reclaimed after the EVM drops.
        let buffer = Rc::new(RefCell::new(std::mem::take(&mut self.reusable_buffer)));
        let local = LocalContext {
            shared_memory_buffer: Rc::clone(&buffer),
            precompile_error_message: None,
        };

        let outcome = {
            let mut evm = self.build_evm_with_inspector_local(inspector, local);

            use revm::context_interface::JournalTr;
            let checkpoint = evm.journaled_state.checkpoint();

            let result = evm
                .inspect_one_tx(tx)
                .map_err(|e| SimError::Other(anyhow!("Failed to transact: {:?}", e)));

            match result {
                Ok(ExecutionResult::Success {
                    logs,
                    gas_used,
                    output,
                    ..
                }) => {
                    let token_deltas = if let Some(token_list) = tokens {
                        evm.inspector.balance_deltas_for_tokens(owner, token_list)
                    } else {
                        evm.inspector.balance_deltas(owner)
                    };

                    // Extract EIP-2930 access list from journaled state
                    let access_list = extract_access_list(&evm.journaled_state.state);

                    if commit {
                        evm.commit_inner();
                    } else {
                        evm.journaled_state.checkpoint_revert(checkpoint);
                    }

                    Ok(CallSimulationResult {
                        status: SimStatus::Success,
                        gas_used,
                        token_deltas,
                        logs,
                        access_list,
                        output: output.into_data(),
                    })
                }
                Ok(ExecutionResult::Revert { gas_used, output }) => {
                    evm.journaled_state.checkpoint_revert(checkpoint);
                    Err(SimulationError::from_revert(gas_used, output).into())
                }
                Ok(ExecutionResult::Halt { reason, gas_used }) => {
                    evm.journaled_state.checkpoint_revert(checkpoint);
                    Err(SimError::Halt {
                        reason: format!("{reason:?}"),
                        gas_used,
                    })
                }
                Err(err) => {
                    evm.journaled_state.checkpoint_revert(checkpoint);
                    Err(err)
                }
            }
        };

        self.reclaim_buffer(buffer);
        outcome
    }

    /// Run a single call with a caller-supplied [`Inspector`](revm::Inspector),
    /// returning the raw [`ExecutionResult`] and handing the inspector back for the
    /// caller to read.
    ///
    /// This is the inspector-generic public seam: where
    /// [`Self::simulate_with_transfer_tracking`] hard-wires the
    /// [`TransferInspector`], this accepts any
    /// [`revm::Inspector`] — a [`CallTracer`](crate::tracing::CallTracer), an
    /// [`InspectorStack`](crate::tracing::InspectorStack) composing several, or a
    /// caller-defined one. It honors a full [`TxConfig`] (value/gas/nonce/access
    /// list) exactly like [`Self::call_raw_with_access_list_with`] and recycles the
    /// reusable shared-memory buffer like the other call methods.
    ///
    /// Unlike `simulate_with_transfer_tracking`, a revert or halt is **not** an
    /// error: the raw [`ExecutionResult`] variant
    /// ([`Success`](ExecutionResult::Success) /
    /// [`Revert`](ExecutionResult::Revert) / [`Halt`](ExecutionResult::Halt)) is
    /// returned as `Ok` so the inspector's captured frames (e.g. a reverted call
    /// tree) remain observable. Only a tx-env build failure or a transact/database
    /// error yields `Err`.
    ///
    /// On a successful transact the journaled changes are either committed into the
    /// overlay's dirty layer (`commit == true`) or reverted (`commit == false`),
    /// matching [`Self::simulate_with_transfer_tracking`]. On a revert/halt the
    /// checkpoint is always reverted regardless of `commit`, so a failed call never
    /// mutates this overlay. On a transact error the checkpoint is reverted too.
    ///
    /// # Errors
    ///
    /// Returns an error if the [`TxEnv`] cannot be built from `from`/`to`/`tx`, or
    /// if revm fails to transact the call (e.g. a database error while loading
    /// state).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use std::sync::Arc;
    /// # use alloy_primitives::{Address, Bytes};
    /// # use evm_fork_cache::cache::{EvmOverlay, EvmSnapshot, TxConfig};
    /// # use evm_fork_cache::CallTracer;
    /// # fn run(snapshot: Arc<EvmSnapshot>, to: Address) -> anyhow::Result<()> {
    /// let mut overlay = EvmOverlay::new(snapshot, None);
    /// let (result, tracer) = overlay.call_raw_with_inspector(
    ///     Address::ZERO,
    ///     to,
    ///     Bytes::new(),
    ///     &TxConfig::default(),
    ///     CallTracer::new(),
    ///     false,
    /// )?;
    /// let _ = result;
    /// let _trace = tracer.into_trace();
    /// # Ok(())
    /// # }
    /// ```
    pub fn call_raw_with_inspector<I>(
        &mut self,
        from: Address,
        to: Address,
        calldata: Bytes,
        tx: &TxConfig,
        inspector: I,
        commit: bool,
    ) -> SimulationResult<(ExecutionResult, I)>
    where
        I: for<'a> revm::Inspector<
                Context<
                    BlockEnv,
                    TxEnv,
                    CfgEnv,
                    &'a mut EvmOverlay,
                    Journal<&'a mut EvmOverlay>,
                    (),
                >,
            >,
    {
        let mut builder = TxEnv::builder()
            .caller(from)
            .kind(TxKind::Call(to))
            .data(calldata)
            .value(tx.value);
        if let Some(gas_limit) = tx.gas_limit {
            builder = builder.gas_limit(gas_limit);
        }
        if let Some(gas_price) = tx.gas_price {
            builder = builder.gas_price(gas_price);
        }
        if let Some(nonce) = tx.nonce {
            builder = builder.nonce(nonce);
        }
        if let Some(access_list) = &tx.access_list {
            builder = builder.access_list(access_list.clone());
        }
        let tx_env = builder
            .build()
            .map_err(|e| SimError::Other(anyhow!("Failed to build tx env: {:?}", e)))?;

        // Recycle the reusable buffer (Pillar A.2); reclaimed after the EVM drops.
        let buffer = Rc::new(RefCell::new(std::mem::take(&mut self.reusable_buffer)));
        let local = LocalContext {
            shared_memory_buffer: Rc::clone(&buffer),
            precompile_error_message: None,
        };

        let outcome = {
            let mut evm = self.build_evm_with_inspector_local(inspector, local);

            use revm::context_interface::JournalTr;
            let checkpoint = evm.journaled_state.checkpoint();

            match evm.inspect_one_tx(tx_env) {
                Ok(result) => {
                    if commit && matches!(result, ExecutionResult::Success { .. }) {
                        evm.commit_inner();
                    } else {
                        evm.journaled_state.checkpoint_revert(checkpoint);
                    }
                    // Hand the inspector back to the caller.
                    Ok((result, evm.inspector))
                }
                Err(e) => {
                    evm.journaled_state.checkpoint_revert(checkpoint);
                    Err(SimError::Other(anyhow!("Failed to transact: {:?}", e)))
                }
            }
        };

        self.reclaim_buffer(buffer);
        outcome
    }

    /// Apply `txs` in order against this overlay over **cumulative** block state,
    /// with a revert policy and coinbase/miner-payment accounting (Phase 6
    /// Track A+B).
    ///
    /// Each transaction observes the committed writes of the ones before it:
    /// the bundle runs on a single overlay/EVM with one outer checkpoint plus a
    /// per-transaction inner checkpoint, so it does **not** rebuild a fresh
    /// overlay per transaction. See the [`bundle`](crate::bundle) module for the
    /// public vocabulary ([`BundleTx`], [`BundleOptions`], [`RevertPolicy`],
    /// [`TxOutcome`], [`BundleResult`]).
    ///
    /// # Revert policy
    ///
    /// - [`RevertPolicy::Atomic`]: the first transaction that reverts/halts
    ///   rolls the whole bundle back to the outer checkpoint, sets
    ///   `succeeded = false`, and stops (`per_tx` ends at the failing
    ///   transaction). `coinbase_payment` is `0` and the overlay is unchanged.
    /// - [`RevertPolicy::AllowReverts`]: a revert at a whitelisted index rolls
    ///   back only that transaction (inner checkpoint) and execution continues;
    ///   a revert at a non-whitelisted index behaves like `Atomic`.
    ///
    /// # Coinbase accounting
    ///
    /// `coinbase_payment` is the block beneficiary's balance delta across the kept
    /// transactions. Under EIP-1559 revm credits the beneficiary only the priority
    /// fee (`(effective_gas_price − basefee) × gas_used`) and burns the base fee
    /// in-EVM, so the delta is the honest miner payment (plus any direct coinbase
    /// tips). Saturating.
    ///
    /// # Commit semantics
    ///
    /// `opts.commit == true` folds the bundle's cumulative state into this
    /// overlay's dirty layer (observable by subsequent overlay calls);
    /// `false` reverts the outer checkpoint so the overlay is unchanged. A
    /// failed atomic bundle never leaves partial state regardless of `commit`.
    ///
    /// # Errors
    ///
    /// Returns [`SimError`] if a transaction environment cannot be built or revm
    /// fails to transact (e.g. a database error). A transaction *reverting* is
    /// not an error — it is reported through the per-transaction
    /// [`TxOutcome`] and the revert policy.
    pub fn simulate_bundle(
        &mut self,
        txs: &[BundleTx],
        opts: &BundleOptions,
    ) -> SimulationResult<BundleResult> {
        // Build every TxEnv up front so a build failure surfaces as an error
        // before we touch the EVM/journal (and the borrow of `self` is clean).
        let tx_envs: Vec<TxEnv> = txs
            .iter()
            .map(|bt| {
                let mut builder = TxEnv::builder()
                    .caller(bt.from)
                    .kind(TxKind::Call(bt.to))
                    .data(bt.calldata.clone())
                    .value(bt.tx.value);
                if let Some(gas_limit) = bt.tx.gas_limit {
                    builder = builder.gas_limit(gas_limit);
                }
                if let Some(gas_price) = bt.tx.gas_price {
                    builder = builder.gas_price(gas_price);
                }
                if let Some(nonce) = bt.tx.nonce {
                    builder = builder.nonce(nonce);
                }
                if let Some(access_list) = &bt.tx.access_list {
                    builder = builder.access_list(access_list.clone());
                }
                builder
                    .build()
                    .map_err(|e| SimError::Other(anyhow!("Failed to build tx env: {:?}", e)))
            })
            .collect::<Result<_, _>>()?;

        // Resolve the beneficiary and read its pre-bundle balance before the
        // mutable borrow of `self` by the EVM (the post-bundle delta is the miner
        // payment; revm already burns the base fee per EIP-1559).
        let beneficiary = self
            .snapshot
            .coinbase
            .unwrap_or_else(|| revm::context::BlockEnv::default().beneficiary);
        let pre_beneficiary_balance = self
            .basic(beneficiary)
            .map_err(|e| SimError::Other(anyhow!("Failed to load beneficiary: {:?}", e)))?
            .map(|info| info.balance)
            .unwrap_or(U256::ZERO);

        // Recycle the reusable buffer (Pillar A.2); reclaimed after the EVM drops.
        let buffer = Rc::new(RefCell::new(std::mem::take(&mut self.reusable_buffer)));
        let local = LocalContext {
            shared_memory_buffer: Rc::clone(&buffer),
            precompile_error_message: None,
        };

        let outcome = {
            use revm::context_interface::JournalTr;
            let mut evm = self.build_evm_with_local(local);

            // Outer checkpoint: the whole-bundle savepoint.
            let outer = evm.journaled_state.checkpoint();

            let mut per_tx: Vec<TxOutcome> = Vec::with_capacity(tx_envs.len());
            let mut total_gas: u64 = 0;
            let mut aborted = false;

            'bundle: for (idx, tx_env) in tx_envs.into_iter().enumerate() {
                // Inner checkpoint: this transaction's savepoint.
                let inner = evm.journaled_state.checkpoint();
                let result = match evm.transact_one(tx_env) {
                    Ok(result) => result,
                    Err(e) => {
                        // Host/transact error: undo this tx and the whole bundle,
                        // reclaim the buffer, and surface as SimError.
                        evm.journaled_state.checkpoint_revert(inner);
                        evm.journaled_state.checkpoint_revert(outer);
                        drop(evm);
                        self.reclaim_buffer(buffer);
                        return Err(SimError::Other(anyhow!("Failed to transact: {:?}", e)));
                    }
                };

                let gas_used = result.gas_used();
                let reverted = !result.is_success();
                let logs = result.logs().to_vec();
                total_gas = total_gas.saturating_add(gas_used);

                per_tx.push(TxOutcome {
                    result,
                    gas_used,
                    reverted,
                    logs,
                });

                if reverted {
                    let allowed = match &opts.revert_policy {
                        RevertPolicy::Atomic => false,
                        RevertPolicy::AllowReverts(idxs) => idxs.contains(&idx),
                    };
                    if allowed {
                        // Roll back only this transaction; later txs still run.
                        evm.journaled_state.checkpoint_revert(inner);
                        continue 'bundle;
                    } else {
                        // Atomic abort: roll the whole bundle back and stop.
                        evm.journaled_state.checkpoint_revert(outer);
                        aborted = true;
                        break 'bundle;
                    }
                }
                // Successful tx: its effects stay journaled for the next tx.
            }

            if aborted {
                // State is reverted to the pre-bundle outer checkpoint regardless
                // of `commit`; no payment.
                BundleResult {
                    per_tx,
                    coinbase_payment: U256::ZERO,
                    gas_used: total_gas,
                    succeeded: false,
                }
            } else {
                // Read the beneficiary's post-bundle balance from the journaled
                // state (present iff it was touched) BEFORE commit/revert, since
                // `commit_inner` finalizes (drains) the journal and an outer
                // revert would undo the credit.
                let post_beneficiary_balance = evm
                    .journaled_state
                    .state
                    .get(&beneficiary)
                    .map(|acct| acct.info.balance)
                    .unwrap_or(pre_beneficiary_balance);
                // revm already excludes the base fee from the beneficiary credit
                // (EIP-1559), so the delta is the honest miner payment.
                let coinbase_payment =
                    post_beneficiary_balance.saturating_sub(pre_beneficiary_balance);

                if opts.commit {
                    evm.commit_inner();
                } else {
                    evm.journaled_state.checkpoint_revert(outer);
                }

                BundleResult {
                    per_tx,
                    coinbase_payment,
                    gas_used: total_gas,
                    succeeded: true,
                }
            }
        };

        self.reclaim_buffer(buffer);
        Ok(outcome)
    }

    /// Execute a non-committing call and return the result plus the touched
    /// [`StorageAccessList`].
    ///
    /// The access list is collected from every account marked touched in the
    /// journaled state after execution, recording both the touched accounts and
    /// the storage slots accessed under each.
    ///
    /// The EVM state is reverted to a checkpoint after a successful transact on
    /// both success and revert/halt outcomes, so the call never mutates this
    /// overlay's dirty layer and each overlay simulation stays isolated. As with
    /// [`Self::call_raw`], a revert or halt is reported through the returned
    /// [`ExecutionResult`] rather than as an error.
    ///
    /// # Errors
    ///
    /// Returns an error if the [`TxEnv`] cannot be built, or if revm fails to
    /// transact the call (for example a database error while loading state).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use std::sync::Arc;
    /// # use alloy_primitives::{Address, Bytes};
    /// # use evm_fork_cache::cache::{EvmOverlay, EvmSnapshot};
    /// # fn run(snapshot: Arc<EvmSnapshot>) -> anyhow::Result<()> {
    /// let mut overlay = EvmOverlay::new(snapshot, None);
    /// let (result, access_list) =
    ///     overlay.call_raw_with_access_list(Address::ZERO, Address::ZERO, Bytes::new())?;
    /// # let _ = (result, access_list);
    /// # Ok(())
    /// # }
    /// ```
    pub fn call_raw_with_access_list(
        &mut self,
        from: Address,
        to: Address,
        calldata: Bytes,
    ) -> Result<(ExecutionResult, StorageAccessList)> {
        self.call_raw_with_access_list_with(from, to, calldata, &TxConfig::default())
    }

    /// Like [`call_raw_with_access_list`](Self::call_raw_with_access_list) but
    /// honors a full [`TxConfig`]: native `value`, `gas_limit`, `gas_price`,
    /// `nonce`, and a pre-warming EIP-2930 `access_list`.
    ///
    /// This is what the freshness optimistic loop uses so a [`SimRequest`]'s tx
    /// environment — e.g. a payable call carrying `value`, or a gas-bounded call
    /// — is reproduced faithfully instead of silently running as a zero-value,
    /// default-gas call. Like the shorthand it is non-committing (the checkpoint
    /// is reverted) and returns the captured storage access list.
    ///
    /// [`SimRequest`]: crate::freshness::SimRequest
    pub fn call_raw_with_access_list_with(
        &mut self,
        from: Address,
        to: Address,
        calldata: Bytes,
        tx: &TxConfig,
    ) -> Result<(ExecutionResult, StorageAccessList)> {
        let mut builder = TxEnv::builder()
            .caller(from)
            .kind(TxKind::Call(to))
            .data(calldata)
            .value(tx.value);
        if let Some(gas_limit) = tx.gas_limit {
            builder = builder.gas_limit(gas_limit);
        }
        if let Some(gas_price) = tx.gas_price {
            builder = builder.gas_price(gas_price);
        }
        if let Some(nonce) = tx.nonce {
            builder = builder.nonce(nonce);
        }
        if let Some(access_list) = &tx.access_list {
            builder = builder.access_list(access_list.clone());
        }
        let tx_env = builder
            .build()
            .map_err(|e| anyhow!("Failed to build tx env: {:?}", e))?;

        // Recycle the reusable buffer (Pillar A.2); reclaimed after the EVM drops.
        let buffer = Rc::new(RefCell::new(std::mem::take(&mut self.reusable_buffer)));
        let local = LocalContext {
            shared_memory_buffer: Rc::clone(&buffer),
            precompile_error_message: None,
        };

        let outcome = {
            let mut evm = self.build_evm_with_local(local);
            use revm::context_interface::JournalTr;
            let checkpoint = evm.journaled_state.checkpoint();
            match evm.transact_one(tx_env) {
                Ok(result) => {
                    let mut access_list = StorageAccessList::default();
                    for (address, account) in evm.journaled_state.state.iter() {
                        if account.is_touched() {
                            access_list.accounts.insert(*address);
                            for (slot_key, _) in account.storage.iter() {
                                access_list.slots.insert((*address, *slot_key));
                            }
                        }
                    }
                    evm.journaled_state.checkpoint_revert(checkpoint);
                    Ok((result, access_list))
                }
                Err(e) => {
                    // Revert the checkpoint even on a host/transact error so the EVM
                    // journal is not left dirty (mirrors `call_raw`).
                    evm.journaled_state.checkpoint_revert(checkpoint);
                    Err(anyhow!("Failed to transact: {:?}", e))
                }
            }
        };

        self.reclaim_buffer(buffer);
        outcome
    }

    /// Write a storage value into this overlay's dirty layer.
    ///
    /// The dirty layer takes precedence over the snapshot on subsequent reads
    /// (see the lookup order on [`EvmOverlay`]), so this injects a value into a
    /// snapshot-backed overlay without mutating the shared snapshot.
    ///
    /// # Freshness validation
    ///
    /// This is the freshness validator's correction step. When a slot the
    /// snapshot captured is found to be stale, the validator writes the
    /// freshly-fetched value here and then re-runs the simulation (e.g. via
    /// [`Self::call_raw`]): the re-run reads the corrected slot out of the dirty
    /// layer instead of the stale snapshot value, so the corrected result
    /// becomes observable. Because the override lives only in this overlay,
    /// other overlays sharing the same `Arc<EvmSnapshot>` are unaffected.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use std::sync::Arc;
    /// # use alloy_primitives::{Address, Bytes, U256};
    /// # use evm_fork_cache::cache::{EvmOverlay, EvmSnapshot};
    /// # fn run(snapshot: Arc<EvmSnapshot>, token: Address, slot: U256) -> anyhow::Result<()> {
    /// let mut overlay = EvmOverlay::new(snapshot, None);
    /// // Inject the fresh value, then re-run to observe the corrected result.
    /// overlay.override_slot(token, slot, U256::from(42u64));
    /// let corrected = overlay.call_raw(Address::ZERO, token, Bytes::new())?;
    /// # let _ = corrected;
    /// # Ok(())
    /// # }
    /// ```
    pub fn override_slot(&mut self, address: Address, slot: U256, value: U256) {
        self.dirty_storage
            .entry(address)
            .or_default()
            .insert(slot, value);
    }
}

impl revm::database_interface::DatabaseCommit for EvmOverlay {
    fn commit(&mut self, changes: alloy_primitives::map::HashMap<Address, revm::state::Account>) {
        for (address, account) in changes {
            self.dirty_accounts.insert(address, account.info);
            let storage = self.dirty_storage.entry(address).or_default();
            for (slot, value) in account.storage {
                storage.insert(slot, value.present_value);
            }
        }
    }
}

impl Database for EvmOverlay {
    type Error = DatabaseError;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        // 1. Check dirty layer
        if let Some(info) = self.dirty_accounts.get(&address) {
            return Ok(Some(info.clone()));
        }
        // 2. Check snapshot (O(1) HashMap lookup, no locks). `account_info` folds
        //    the two snapshot tiers (overlay ▸ base) and already short-circuits a
        //    NotExisting account to None — it must NOT fall through to the ext_db,
        //    mirroring revm `DbAccount::info()` and the live `EvmCache` read.
        if self.snapshot.accounts_not_existing.contains(&address) {
            return Ok(None);
        }
        if let Some(info) = self.snapshot.account_info(address) {
            return Ok(Some(info.clone()));
        }
        // 3. RPC fallback
        if let Some(ref ext_db) = self.ext_db {
            let info = ext_db.basic_ref(address)?;
            if let Some(ref info) = info {
                self.dirty_accounts.insert(address, info.clone());
            }
            return Ok(info);
        }
        Ok(None)
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        // Check dirty accounts first
        for info in self.dirty_accounts.values() {
            if info.code_hash == code_hash
                && let Some(code) = &info.code
            {
                return Ok(code.clone());
            }
        }
        // Check the snapshot's code index (overlay ▸ base).
        if let Some(code) = self.snapshot.code(code_hash) {
            return Ok(code.clone());
        }
        // RPC fallback
        if let Some(ref ext_db) = self.ext_db {
            return ext_db.code_by_hash_ref(code_hash);
        }
        Ok(Bytecode::default())
    }

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, Self::Error> {
        // 1. Check dirty layer
        if let Some(account_storage) = self.dirty_storage.get(&address)
            && let Some(value) = account_storage.get(&index)
        {
            return Ok(*value);
        }
        // 2. Check snapshot (O(1)). `storage_value` folds the two tiers (overlay ▸
        //    cleared-as-ZERO ▸ base); a cleared account's absent slot reads ZERO
        //    and must NOT fall through to the ext_db, mirroring the live EVM SLOAD
        //    for a StorageCleared/NotExisting account.
        if let Some(value) = self.snapshot.storage_value(address, index) {
            return Ok(value);
        }
        // 3. RPC fallback
        if let Some(ref ext_db) = self.ext_db {
            let value = ext_db.storage_ref(address, index)?;
            self.dirty_storage
                .entry(address)
                .or_default()
                .insert(index, value);
            return Ok(value);
        }
        Ok(U256::ZERO)
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        if let Some(hash) = self.snapshot.block_hashes.get(&number) {
            return Ok(*hash);
        }
        if let Some(ref ext_db) = self.ext_db {
            return ext_db.block_hash_ref(number);
        }
        // Snapshots never populate `block_hashes` (the live cache does not track
        // block hashes), so without an `ext_db` the `BLOCKHASH` opcode resolves to
        // ZERO. Overlays built internally (e.g. the freshness validator) pass
        // `ext_db = None`; a contract that reads `BLOCKHASH` through such an
        // overlay sees ZERO. Documented in docs/KNOWN_ISSUES.md.
        Ok(B256::ZERO)
    }
}

fn extract_access_list(state: &revm::state::EvmState) -> AccessList {
    let items: Vec<AccessListItem> = state
        .iter()
        .filter(|(_, account)| account.is_touched())
        .map(|(address, account)| AccessListItem {
            address: *address,
            storage_keys: account
                .storage
                .keys()
                .map(|slot| B256::from(*slot))
                .collect(),
        })
        .collect();
    AccessList(items)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::snapshot::BaseState;
    use revm::primitives::hardfork::SpecId;
    use std::collections::HashSet;

    /// Build a two-tier `EvmSnapshot` whose cold base holds the given accounts,
    /// storage, and code, with an empty hot overlay — the shape
    /// `create_snapshot_deep_clone` produces. The `Arc`-per-account storage of the
    /// base is built from the plain per-account maps.
    fn snap(
        accounts: HashMap<Address, AccountInfo>,
        storage: HashMap<Address, HashMap<U256, U256>>,
        code_by_hash: HashMap<B256, Bytecode>,
        block_hashes: HashMap<u64, B256>,
    ) -> Arc<EvmSnapshot> {
        let base = BaseState {
            accounts,
            storage: storage
                .into_iter()
                .map(|(addr, slots)| (addr, Arc::new(slots)))
                .collect(),
            code_by_hash,
        };
        Arc::new(EvmSnapshot {
            base: Arc::new(base),
            overlay_accounts: HashMap::new(),
            overlay_storage: HashMap::new(),
            overlay_code_by_hash: HashMap::new(),
            storage_cleared: HashSet::new(),
            accounts_not_existing: HashSet::new(),
            block_hashes,
            block_number: None,
            basefee: None,
            coinbase: None,
            prevrandao: None,
            gas_limit: None,
            chain_id: 42161,
            timestamp: None,
            spec_id: SpecId::CANCUN,
            shared_memory_capacity: 64_000,
        })
    }

    #[test]
    fn test_overlay_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<EvmOverlay>();
    }

    #[test]
    fn test_overlay_basic_from_snapshot() {
        let mut accounts = HashMap::new();
        let info = AccountInfo {
            balance: U256::from(1000),
            nonce: 1,
            code_hash: B256::ZERO,
            code: None,
            account_id: None,
        };
        let addr = Address::repeat_byte(0x01);
        accounts.insert(addr, info);

        let snapshot = snap(accounts, HashMap::new(), HashMap::new(), HashMap::new());

        let mut overlay = EvmOverlay::new(snapshot, None);
        let result = overlay.basic(addr).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().balance, U256::from(1000));
    }

    #[test]
    fn test_overlay_storage_from_snapshot() {
        let addr = Address::repeat_byte(0x01);
        let slot = U256::from(42);
        let value = U256::from(999);

        let mut storage = HashMap::new();
        let mut account_storage = HashMap::new();
        account_storage.insert(slot, value);
        storage.insert(addr, account_storage);

        let snapshot = snap(HashMap::new(), storage, HashMap::new(), HashMap::new());

        let mut overlay = EvmOverlay::new(snapshot, None);
        let result = overlay.storage(addr, slot).unwrap();
        assert_eq!(result, value);
    }

    #[test]
    fn test_overlay_dirty_overrides_snapshot() {
        let addr = Address::repeat_byte(0x01);
        let slot = U256::from(42);

        let mut storage = HashMap::new();
        let mut account_storage = HashMap::new();
        account_storage.insert(slot, U256::from(100));
        storage.insert(addr, account_storage);

        let snapshot = snap(HashMap::new(), storage, HashMap::new(), HashMap::new());

        let mut overlay = EvmOverlay::new(snapshot, None);

        // Write to dirty layer
        overlay
            .dirty_storage
            .entry(addr)
            .or_default()
            .insert(slot, U256::from(200));

        // Should read dirty value, not snapshot
        let result = overlay.storage(addr, slot).unwrap();
        assert_eq!(result, U256::from(200));
    }

    #[test]
    fn test_overlay_missing_returns_zero() {
        let snapshot = snap(
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        );

        let mut overlay = EvmOverlay::new(snapshot, None);
        let addr = Address::repeat_byte(0x99);
        let result = overlay.storage(addr, U256::from(1)).unwrap();
        assert_eq!(result, U256::ZERO);

        let account = overlay.basic(addr).unwrap();
        assert!(account.is_none());
    }

    #[test]
    fn test_overlay_code_by_hash_from_snapshot() {
        let code = Bytecode::new_raw(Bytes::from(vec![0x60, 0x00, 0x60, 0x00]));
        let hash = code.hash_slow();

        let mut code_by_hash = HashMap::new();
        code_by_hash.insert(hash, code.clone());

        let snapshot = snap(HashMap::new(), HashMap::new(), code_by_hash, HashMap::new());

        let mut overlay = EvmOverlay::new(snapshot, None);
        let result = overlay.code_by_hash(hash).unwrap();
        assert_eq!(result.len(), 4);
    }

    #[test]
    fn test_overlay_block_hash() {
        let mut block_hashes = HashMap::new();
        let hash = B256::repeat_byte(0xAB);
        block_hashes.insert(42u64, hash);

        let snapshot = snap(HashMap::new(), HashMap::new(), HashMap::new(), block_hashes);

        let mut overlay = EvmOverlay::new(snapshot, None);
        assert_eq!(overlay.block_hash(42).unwrap(), hash);
        assert_eq!(overlay.block_hash(99).unwrap(), B256::ZERO);
    }
}
