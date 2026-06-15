//! Binary EVM state persistence for fast startup.
//!
//! Provides a bincode-serialized alternative to foundry-fork-db's JSON format.
//! On save, we extract accounts (without bytecode) and storage from BlockchainDb
//! and write a compact binary file. On load, we populate BlockchainDb directly,
//! then seed bytecodes from the separate bytecodes.bin cache.
//!
//! The file format is raw bincode with no version header or magic bytes, so it
//! is not migratable: a cache written by a build with a different struct layout
//! decodes as a failure (cache miss) rather than being upgraded in place.

use std::path::Path;
use std::time::Instant;

use alloy_primitives::map::HashMap;
use alloy_primitives::{Address, B256, U256};
use foundry_fork_db::BlockchainDb;
use revm::state::AccountInfo;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

/// Binary-serializable EVM state. Stores accounts without bytecode (bytecodes
/// are loaded separately from bytecodes.bin) and all storage slots.
#[derive(Serialize, Deserialize)]
struct BinaryEvmState {
    accounts: Vec<(Address, BinaryAccountInfo)>,
    storage: Vec<(Address, Vec<(U256, U256)>)>,
}

/// Compact account info without bytecode.
#[derive(Serialize, Deserialize)]
struct BinaryAccountInfo {
    balance: U256,
    nonce: u64,
    code_hash: B256,
}

/// Save the current BlockchainDb state to a binary file.
///
/// This extracts accounts (without code) and storage from the MemDb
/// and serializes them with bincode for fast restoration. Bytecode is excluded
/// and persisted separately to `bytecodes.bin`; the saved account info keeps
/// only the `code_hash`.
///
/// Errors are logged at `warn` level and otherwise swallowed: serialization
/// failures, parent-directory creation failures, and write failures all return
/// without signalling to the caller, so a failed save is indistinguishable from
/// a successful one at the call site.
///
/// The on-disk format is raw bincode with no version header, so it is not
/// forward/backward compatible: a file written by a build with a different
/// layout will fail to decode on load (treated as a cache miss) rather than
/// being migrated.
pub fn save_binary_state(blockchain_db: &BlockchainDb, path: &Path) {
    let start = Instant::now();

    let accounts: Vec<(Address, BinaryAccountInfo)> = blockchain_db
        .accounts()
        .read()
        .iter()
        .map(|(addr, info)| {
            (
                *addr,
                BinaryAccountInfo {
                    balance: info.balance,
                    nonce: info.nonce,
                    code_hash: info.code_hash,
                },
            )
        })
        .collect();

    let storage: Vec<(Address, Vec<(U256, U256)>)> = blockchain_db
        .storage()
        .read()
        .iter()
        .map(|(addr, slots)| (*addr, slots.iter().map(|(k, v)| (*k, *v)).collect()))
        .collect();

    let state = BinaryEvmState { accounts, storage };

    match bincode::serialize(&state) {
        Ok(data) => {
            if let Some(parent) = path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            match std::fs::write(path, &data) {
                Ok(()) => {
                    let ms = start.elapsed().as_millis();
                    debug!(
                        accounts = state.accounts.len(),
                        storage_contracts = state.storage.len(),
                        bytes = data.len(),
                        save_ms = ms,
                        "Saved binary EVM state"
                    );
                }
                Err(e) => warn!(error = %e, "Failed to write binary EVM state"),
            }
        }
        Err(e) => warn!(error = %e, "Failed to serialize binary EVM state"),
    }
}

/// Load binary EVM state and populate the BlockchainDb.
///
/// Returns `true` if the binary state was loaded successfully, `false` otherwise.
/// When successful, accounts (without code) and storage are populated in the MemDb.
/// Bytecodes should be seeded separately from bytecodes.bin.
///
/// Returns `false` (rather than erroring) when `path` cannot be read or its
/// contents fail to decode as the expected bincode layout; a decode failure is
/// logged at `warn` level. Because the format carries no version header, a file
/// written by an incompatible build is reported as a decode failure here.
pub fn load_binary_state(blockchain_db: &BlockchainDb, path: &Path) -> bool {
    let start = Instant::now();

    let data = match std::fs::read(path) {
        Ok(d) => d,
        Err(_) => return false,
    };

    let state: BinaryEvmState = match bincode::deserialize(&data) {
        Ok(s) => s,
        Err(e) => {
            warn!(?e, "Failed to decode binary EVM state, starting fresh");
            return false;
        }
    };

    let account_count = state.accounts.len();
    let storage_contract_count = state.storage.len();
    let mut total_slots = 0usize;

    // Populate accounts (without code — bytecodes loaded separately)
    {
        let mut accounts = blockchain_db.accounts().write();
        for (addr, info) in state.accounts {
            accounts.insert(
                addr,
                AccountInfo {
                    balance: info.balance,
                    nonce: info.nonce,
                    code_hash: info.code_hash,
                    code: None,
                    account_id: None,
                },
            );
        }
    }

    // Populate storage
    {
        let mut storage = blockchain_db.storage().write();
        for (addr, slots) in state.storage {
            total_slots += slots.len();
            let map: HashMap<U256, U256> = slots.into_iter().collect();
            storage.insert(addr, map);
        }
    }

    let ms = start.elapsed().as_millis();
    debug!(
        accounts = account_count,
        storage_contracts = storage_contract_count,
        total_slots,
        bytes = data.len(),
        load_ms = ms,
        "Loaded binary EVM state"
    );

    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use foundry_fork_db::cache::BlockchainDbMeta;

    #[test]
    fn test_save_load_round_trip() {
        let dir = std::env::temp_dir().join("evm_fork_cache_test_binary_state");
        let path = dir.join("test_state.bin");
        let _ = std::fs::remove_file(&path);

        // Create a BlockchainDb with some data
        let meta = BlockchainDbMeta::default();
        let db = BlockchainDb::new(meta, None);

        let addr1 = Address::repeat_byte(0x01);
        let addr2 = Address::repeat_byte(0x02);

        // Add accounts
        {
            let mut accounts = db.accounts().write();
            accounts.insert(
                addr1,
                AccountInfo {
                    balance: U256::from(1000),
                    nonce: 5,
                    code_hash: B256::repeat_byte(0xAA),
                    code: None,
                    account_id: None,
                },
            );
            accounts.insert(
                addr2,
                AccountInfo {
                    balance: U256::from(2000),
                    nonce: 10,
                    code_hash: B256::repeat_byte(0xBB),
                    code: None,
                    account_id: None,
                },
            );
        }

        // Add storage
        {
            let mut storage = db.storage().write();
            let mut slots1 = HashMap::default();
            slots1.insert(U256::from(0), U256::from(42));
            slots1.insert(U256::from(1), U256::from(99));
            storage.insert(addr1, slots1);

            let mut slots2 = HashMap::default();
            slots2.insert(U256::from(4), U256::from(777));
            storage.insert(addr2, slots2);
        }

        // Save
        save_binary_state(&db, &path);
        assert!(path.exists());

        // Load into a fresh db
        let meta2 = BlockchainDbMeta::default();
        let db2 = BlockchainDb::new(meta2, None);
        assert!(load_binary_state(&db2, &path));

        // Verify accounts
        {
            let accounts = db2.accounts().read();
            assert_eq!(accounts.len(), 2);
            let info1 = accounts.get(&addr1).unwrap();
            assert_eq!(info1.balance, U256::from(1000));
            assert_eq!(info1.nonce, 5);
            assert!(info1.code.is_none()); // code not stored in binary
        }

        // Verify storage
        {
            let storage = db2.storage().read();
            assert_eq!(storage.len(), 2);
            assert_eq!(
                *storage.get(&addr1).unwrap().get(&U256::from(0)).unwrap(),
                U256::from(42)
            );
            assert_eq!(
                *storage.get(&addr2).unwrap().get(&U256::from(4)).unwrap(),
                U256::from(777)
            );
        }

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir(&dir);
    }

    #[test]
    fn test_load_missing_file_returns_false() {
        let meta = BlockchainDbMeta::default();
        let db = BlockchainDb::new(meta, None);
        assert!(!load_binary_state(
            &db,
            std::path::Path::new("/tmp/nonexistent_binary_state.bin")
        ));
    }

    #[test]
    fn test_load_corrupt_file_returns_false() {
        let dir = std::env::temp_dir().join("evm_fork_cache_test_binary_state_corrupt");
        let path = dir.join("corrupt.bin");
        let _ = std::fs::create_dir_all(&dir);
        std::fs::write(&path, b"not valid bincode").unwrap();

        let meta = BlockchainDbMeta::default();
        let db = BlockchainDb::new(meta, None);
        assert!(!load_binary_state(&db, &path));

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir(&dir);
    }
}
