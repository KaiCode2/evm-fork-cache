use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;
use std::time::SystemTime;

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
use super::{CallSimulationResult, SimStatus, TxConfig};
use crate::access_set::StorageAccessList;
use crate::errors::{SimError, SimulationError, SimulationResult};
use crate::inspector::TransferInspector;

/// Default initial capacity for shared memory buffer (64KB).
const OVERLAY_SHARED_MEMORY_CAPACITY: usize = 64 * 1024;

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
pub struct EvmOverlay {
    snapshot: Arc<EvmSnapshot>,
    /// Per-simulation mutations (accounts fetched from ext_db, committed changes).
    dirty_accounts: HashMap<Address, AccountInfo>,
    /// Per-simulation storage mutations.
    dirty_storage: HashMap<Address, HashMap<U256, U256>>,
    /// Optional RPC fallback for data not in snapshot.
    ext_db: Option<SharedBackend>,
}

impl EvmOverlay {
    /// Create a new overlay on the given snapshot.
    pub fn new(snapshot: Arc<EvmSnapshot>, ext_db: Option<SharedBackend>) -> Self {
        Self {
            snapshot,
            dirty_accounts: HashMap::new(),
            dirty_storage: HashMap::new(),
            ext_db,
        }
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

    /// Build a revm EVM instance backed by this overlay.
    ///
    /// Note: The returned EVM is `!Send` (due to `LocalContext`'s `Rc<RefCell>`),
    /// but this is fine because it's created and used within a single task.
    pub fn build_evm(&mut self) -> OverlayEvm<'_> {
        let local = LocalContext {
            shared_memory_buffer: Rc::new(RefCell::new(Vec::with_capacity(
                OVERLAY_SHARED_MEMORY_CAPACITY,
            ))),
            precompile_error_message: None,
        };
        // Read snapshot values before the mutable borrow of self
        let chain_id = self.snapshot.chain_id;
        let spec_id = self.snapshot.spec_id;
        let timestamp = self.snapshot.timestamp.unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        });
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

        let mut evm = self.build_evm();
        use revm::context_interface::JournalTr;
        let checkpoint = evm.journaled_state.checkpoint();
        let result = evm
            .transact_one(tx)
            .map_err(|e| anyhow!("Failed to transact: {:?}", e));
        evm.journaled_state.checkpoint_revert(checkpoint);
        result
    }

    /// Build a revm EVM instance with an inspector, backed by this overlay.
    fn build_evm_with_inspector<INSP>(&mut self, inspector: INSP) -> InspectorOverlayEvm<'_, INSP> {
        let local = LocalContext {
            shared_memory_buffer: Rc::new(RefCell::new(Vec::with_capacity(
                OVERLAY_SHARED_MEMORY_CAPACITY,
            ))),
            precompile_error_message: None,
        };
        let chain_id = self.snapshot.chain_id;
        let spec_id = self.snapshot.spec_id;
        let timestamp = self.snapshot.timestamp.unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        });
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
        let mut evm = self.build_evm_with_inspector(inspector);

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

        let mut evm = self.build_evm();
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
        // 2. Check snapshot (O(1) HashMap lookup, no locks)
        if let Some(info) = self.snapshot.accounts.get(&address) {
            return Ok(Some(info.clone()));
        }
        // 2b. A NotExisting account is absent to the EVM: return None and do NOT
        //     fall through to the ext_db, mirroring revm `DbAccount::info()` and the
        //     live `EvmCache` account read (symmetric with `storage_cleared`).
        if self.snapshot.accounts_not_existing.contains(&address) {
            return Ok(None);
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
        // Check snapshot's code_by_hash index
        if let Some(code) = self.snapshot.code_by_hash.get(&code_hash) {
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
        // 2. Check snapshot (O(1))
        if let Some(account_storage) = self.snapshot.storage.get(&address)
            && let Some(value) = account_storage.get(&index)
        {
            return Ok(*value);
        }
        // 2b. A cleared account's storage is locally complete: an absent slot reads
        //     ZERO and must NOT fall through to the ext_db, mirroring the live EVM
        //     SLOAD for a StorageCleared/NotExisting account.
        if self.snapshot.storage_cleared.contains(&address) {
            return Ok(U256::ZERO);
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
    use revm::primitives::hardfork::SpecId;

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

        let snapshot = Arc::new(EvmSnapshot {
            accounts,
            storage: HashMap::new(),
            block_hashes: HashMap::new(),
            storage_cleared: std::collections::HashSet::new(),
            accounts_not_existing: std::collections::HashSet::new(),
            code_by_hash: HashMap::new(),
            block_number: None,
            basefee: None,
            coinbase: None,
            prevrandao: None,
            gas_limit: None,
            chain_id: 42161,
            timestamp: None,
            spec_id: SpecId::CANCUN,
        });

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

        let snapshot = Arc::new(EvmSnapshot {
            accounts: HashMap::new(),
            storage,
            block_hashes: HashMap::new(),
            storage_cleared: std::collections::HashSet::new(),
            accounts_not_existing: std::collections::HashSet::new(),
            code_by_hash: HashMap::new(),
            block_number: None,
            basefee: None,
            coinbase: None,
            prevrandao: None,
            gas_limit: None,
            chain_id: 42161,
            timestamp: None,
            spec_id: SpecId::CANCUN,
        });

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

        let snapshot = Arc::new(EvmSnapshot {
            accounts: HashMap::new(),
            storage,
            block_hashes: HashMap::new(),
            storage_cleared: std::collections::HashSet::new(),
            accounts_not_existing: std::collections::HashSet::new(),
            code_by_hash: HashMap::new(),
            block_number: None,
            basefee: None,
            coinbase: None,
            prevrandao: None,
            gas_limit: None,
            chain_id: 42161,
            timestamp: None,
            spec_id: SpecId::CANCUN,
        });

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
        let snapshot = Arc::new(EvmSnapshot {
            accounts: HashMap::new(),
            storage: HashMap::new(),
            block_hashes: HashMap::new(),
            storage_cleared: std::collections::HashSet::new(),
            accounts_not_existing: std::collections::HashSet::new(),
            code_by_hash: HashMap::new(),
            block_number: None,
            basefee: None,
            coinbase: None,
            prevrandao: None,
            gas_limit: None,
            chain_id: 42161,
            timestamp: None,
            spec_id: SpecId::CANCUN,
        });

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

        let snapshot = Arc::new(EvmSnapshot {
            accounts: HashMap::new(),
            storage: HashMap::new(),
            block_hashes: HashMap::new(),
            storage_cleared: std::collections::HashSet::new(),
            accounts_not_existing: std::collections::HashSet::new(),
            code_by_hash,
            block_number: None,
            basefee: None,
            coinbase: None,
            prevrandao: None,
            gas_limit: None,
            chain_id: 42161,
            timestamp: None,
            spec_id: SpecId::CANCUN,
        });

        let mut overlay = EvmOverlay::new(snapshot, None);
        let result = overlay.code_by_hash(hash).unwrap();
        assert_eq!(result.len(), 4);
    }

    #[test]
    fn test_overlay_block_hash() {
        let mut block_hashes = HashMap::new();
        let hash = B256::repeat_byte(0xAB);
        block_hashes.insert(42u64, hash);

        let snapshot = Arc::new(EvmSnapshot {
            accounts: HashMap::new(),
            storage: HashMap::new(),
            storage_cleared: std::collections::HashSet::new(),
            accounts_not_existing: std::collections::HashSet::new(),
            block_hashes,
            code_by_hash: HashMap::new(),
            block_number: None,
            basefee: None,
            coinbase: None,
            prevrandao: None,
            gas_limit: None,
            chain_id: 42161,
            timestamp: None,
            spec_id: SpecId::CANCUN,
        });

        let mut overlay = EvmOverlay::new(snapshot, None);
        assert_eq!(overlay.block_hash(42).unwrap(), hash);
        assert_eq!(overlay.block_hash(99).unwrap(), B256::ZERO);
    }
}
