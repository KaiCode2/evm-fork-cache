//! On-disk cache of contract bytecode, keyed by account address.
//!
//! Bytecode is large and immutable for a deployed contract, so it is persisted
//! in its own file (`bytecodes.bin`) separately from the binary EVM state. On
//! save we copy the bytecode of every account that has any, and on load these
//! entries are used to re-seed the `code` of accounts that were restored
//! without it.
//!
//! Each entry's bytes are hex-encoded for the serde representation. The file is
//! written as a crate-specific versioned envelope followed by bincode payload, so
//! incompatible versions are detected as cache misses.

use std::collections::HashMap;
use std::path::Path;

use alloy_primitives::Address;
use anyhow::Result;
use foundry_fork_db::BlockchainDb;
use serde::{Deserialize, Serialize};

use super::versioned;

const BYTECODE_CACHE_MAGIC: &[u8; 8] = b"EFCBYTE\0";
const BYTECODE_CACHE_VERSION: u32 = 1;

/// Serializable bytecode cache entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct BytecodeCacheEntry {
    /// Raw bytecode bytes (hex-encoded for JSON).
    #[serde(with = "hex_bytes")]
    pub(crate) bytecode: Vec<u8>,
}

/// Serializable bytecode cache.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct BytecodeCache {
    /// Map of address to bytecode.
    pub(crate) contracts: HashMap<Address, BytecodeCacheEntry>,
}

impl BytecodeCache {
    /// Load bytecode cache from disk (binary format).
    ///
    /// Returns `None` if `path` cannot be read, fails the magic/version check, or
    /// fails to decode as bincode for this type.
    pub(crate) fn load(path: &Path) -> Option<Self> {
        let data = std::fs::read(path).ok()?;
        versioned::decode(
            &data,
            BYTECODE_CACHE_MAGIC,
            BYTECODE_CACHE_VERSION,
            "bytecode cache",
        )
    }

    /// Save bytecode cache to disk (binary format).
    ///
    /// Creates the parent directory if needed, then writes the
    /// bincode-serialized cache to `path`.
    ///
    /// # Errors
    ///
    /// Returns an error if the parent directory cannot be created, if bincode
    /// serialization fails, or if writing the file fails.
    pub(crate) fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let data = versioned::encode(
            BYTECODE_CACHE_MAGIC,
            BYTECODE_CACHE_VERSION,
            self,
            "bytecode cache",
        )?;
        std::fs::write(path, data)?;
        Ok(())
    }

    /// Merge new bytecodes from a BlockchainDb.
    ///
    /// Inserts (or overwrites) an entry for every account that currently has
    /// non-empty `code`; accounts without loaded code are skipped. Existing
    /// entries for addresses not present in `db` are left untouched.
    pub(crate) fn merge_from_db(&mut self, db: &BlockchainDb) {
        let accounts = db.accounts().read();
        for (addr, info) in accounts.iter() {
            // Only cache accounts that have bytecode
            if let Some(code) = &info.code
                && !code.is_empty()
            {
                self.contracts.insert(
                    *addr,
                    BytecodeCacheEntry {
                        bytecode: code.original_byte_slice().to_vec(),
                    },
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Bytes, U256};
    use foundry_fork_db::cache::BlockchainDbMeta;
    use revm::state::{AccountInfo, Bytecode};

    fn temp_path(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("evm_fork_cache_bytecode_{tag}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create temp dir");
        dir.join("bytecodes.bin")
    }

    #[test]
    fn save_load_round_trip_through_hex_serde() {
        let path = temp_path("roundtrip");
        let addr = Address::repeat_byte(0x42);

        let mut cache = BytecodeCache::default();
        cache.contracts.insert(
            addr,
            BytecodeCacheEntry {
                bytecode: vec![0x60, 0x00, 0x60, 0x00, 0xf3],
            },
        );
        cache.save(&path).expect("save bytecode cache");
        let bytes = std::fs::read(&path).expect("read saved bytecode cache");
        assert!(
            bytes.starts_with(b"EFCBYTE\0"),
            "bytecode cache must carry a magic header"
        );
        assert_eq!(
            &bytes[8..12],
            &1u32.to_le_bytes(),
            "bytecode cache must carry an explicit version"
        );

        let loaded = BytecodeCache::load(&path).expect("load bytecode cache");
        assert_eq!(
            loaded.contracts.get(&addr).map(|e| e.bytecode.clone()),
            Some(vec![0x60, 0x00, 0x60, 0x00, 0xf3]),
            "bytecode survives the hex-encoded round trip"
        );

        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn load_legacy_raw_bincode_is_none() {
        let path = temp_path("legacy");
        let mut cache = BytecodeCache::default();
        cache.contracts.insert(
            Address::repeat_byte(0x42),
            BytecodeCacheEntry {
                bytecode: vec![0x60, 0x00],
            },
        );
        std::fs::write(&path, bincode::serialize(&cache).unwrap()).expect("write legacy cache");

        assert!(
            BytecodeCache::load(&path).is_none(),
            "unversioned legacy bincode must be treated as a cache miss"
        );

        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn load_missing_file_is_none() {
        assert!(BytecodeCache::load(std::path::Path::new("/nonexistent/bytecodes.bin")).is_none());
    }

    #[test]
    fn merge_from_db_caches_only_coded_accounts() {
        let db = BlockchainDb::new(BlockchainDbMeta::default(), None);
        let coded = Address::repeat_byte(0x01);
        let eoa = Address::repeat_byte(0x02);

        let code = Bytecode::new_raw(Bytes::from_static(&[0x60, 0x01, 0x60, 0x02, 0x01]));
        let expected = code.original_byte_slice().to_vec();
        let code_hash = code.hash_slow();
        {
            let mut accounts = db.accounts().write();
            accounts.insert(
                coded,
                AccountInfo {
                    balance: U256::ZERO,
                    nonce: 1,
                    code: Some(code),
                    code_hash,
                    account_id: None,
                },
            );
            // An account with no loaded code must be skipped.
            accounts.insert(eoa, AccountInfo::default());
        }

        let mut cache = BytecodeCache::default();
        cache.merge_from_db(&db);

        assert_eq!(cache.contracts.len(), 1, "only the coded account is cached");
        assert_eq!(
            cache.contracts.get(&coded).map(|e| e.bytecode.clone()),
            Some(expected)
        );
        assert!(!cache.contracts.contains_key(&eoa));
    }
}

/// Hex serialization for bytecode bytes.
mod hex_bytes {
    use alloy_primitives::hex;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        format!("0x{}", hex::encode(bytes)).serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
        let s = String::deserialize(deserializer)?;
        let s = s.strip_prefix("0x").unwrap_or(&s);
        hex::decode(s).map_err(serde::de::Error::custom)
    }
}
