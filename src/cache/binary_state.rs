//! Binary EVM state persistence for fast startup.
//!
//! Provides a bincode-serialized alternative to foundry-fork-db's JSON format.
//! On save, we extract accounts (without bytecode) and storage from BlockchainDb
//! and write a compact binary file. On load, we populate BlockchainDb directly,
//! then seed bytecodes from the separate bytecodes.bin cache.
//!
//! The file format is a tiny crate-specific envelope (magic bytes + version)
//! followed by bincode payload. Unknown magic/version values are cache misses.

use std::path::Path;
use std::time::Instant;

use alloy_primitives::map::HashMap;
use alloy_primitives::{Address, B256, U256};
use anyhow::{Context as _, Result};
use foundry_fork_db::BlockchainDb;
use revm::state::AccountInfo;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use super::versioned;

const BINARY_STATE_MAGIC: &[u8; 8] = b"EFCSTAT\0";
const BINARY_STATE_VERSION: u32 = 1;

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
/// Returns an error if serialization, parent-directory creation, or writing
/// fails, so explicit flush callers can distinguish a successful save from a
/// stale or missing on-disk cache.
///
/// The on-disk format carries magic bytes and a version number before the
/// bincode payload. Unknown versions are treated as a cache miss rather than
/// being migrated.
pub fn save_binary_state(blockchain_db: &BlockchainDb, path: &Path) -> Result<()> {
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

    let data = versioned::encode(
        BINARY_STATE_MAGIC,
        BINARY_STATE_VERSION,
        &state,
        "binary EVM state",
    )?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create binary EVM state directory {parent:?}"))?;
    }
    std::fs::write(path, &data)
        .with_context(|| format!("failed to write binary EVM state to {path:?}"))?;

    let ms = start.elapsed().as_millis();
    debug!(
        accounts = state.accounts.len(),
        storage_contracts = state.storage.len(),
        bytes = data.len(),
        save_ms = ms,
        "Saved binary EVM state"
    );
    Ok(())
}

/// Load binary EVM state and populate the BlockchainDb.
///
/// Returns `true` if the binary state was loaded successfully, `false` otherwise.
/// When successful, accounts (without code) and storage are populated in the MemDb.
/// Bytecodes should be seeded separately from bytecodes.bin.
///
/// Returns `false` (rather than erroring) when `path` cannot be read or its
/// contents fail the magic/version check or fail to decode as the expected
/// bincode layout. A missing file (the normal cold-start case) is logged at
/// `debug`; an actual read error (e.g. permission denied) and any
/// magic/version/decode failure are logged at `warn`.
pub fn load_binary_state(blockchain_db: &BlockchainDb, path: &Path) -> bool {
    let start = Instant::now();

    let data = match std::fs::read(path) {
        Ok(d) => d,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            debug!("No binary EVM state file found, starting fresh");
            return false;
        }
        Err(e) => {
            warn!(error = %e, "Failed to read binary EVM state, starting fresh");
            return false;
        }
    };

    let Some(state) = versioned::decode::<BinaryEvmState>(
        &data,
        BINARY_STATE_MAGIC,
        BINARY_STATE_VERSION,
        "binary EVM state",
    ) else {
        warn!("Failed to decode binary EVM state, starting fresh");
        return false;
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
        save_binary_state(&db, &path).expect("save binary state");
        assert!(path.exists());
        let bytes = std::fs::read(&path).expect("read saved state");
        assert!(
            bytes.starts_with(b"EFCSTAT\0"),
            "binary state cache must carry a magic header"
        );
        assert_eq!(
            &bytes[8..12],
            &1u32.to_le_bytes(),
            "binary state cache must carry an explicit version"
        );

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
    fn save_binary_state_reports_write_failures() {
        let dir = std::env::temp_dir().join("evm_fork_cache_test_binary_state_write_error");
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_file(&dir);
        std::fs::write(&dir, b"not a directory").expect("create file path conflict");

        let db = BlockchainDb::new(BlockchainDbMeta::default(), None);
        let path = dir.join("state.bin");
        let err = save_binary_state(&db, &path).expect_err("save must report write failure");
        assert!(
            err.to_string().contains("directory") || err.to_string().contains("Not a directory"),
            "unexpected error: {err:#}"
        );

        let _ = std::fs::remove_file(&dir);
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

    #[test]
    fn load_legacy_raw_bincode_returns_false() {
        let dir = std::env::temp_dir().join("evm_fork_cache_test_binary_state_legacy");
        let path = dir.join("legacy.bin");
        let _ = std::fs::create_dir_all(&dir);
        let legacy = BinaryEvmState {
            accounts: Vec::new(),
            storage: Vec::new(),
        };
        std::fs::write(&path, bincode::serialize(&legacy).unwrap()).unwrap();

        let db = BlockchainDb::new(BlockchainDbMeta::default(), None);
        assert!(
            !load_binary_state(&db, &path),
            "unversioned legacy bincode must be treated as a cache miss"
        );

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir(&dir);
    }
}
