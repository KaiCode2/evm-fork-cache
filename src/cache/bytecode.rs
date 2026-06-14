use std::collections::HashMap;
use std::path::Path;

use alloy_primitives::Address;
use anyhow::Result;
use foundry_fork_db::BlockchainDb;
use serde::{Deserialize, Serialize};
use tracing::warn;

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
    pub(crate) fn load(path: &Path) -> Option<Self> {
        let data = std::fs::read(path).ok()?;
        bincode::deserialize(&data)
            .inspect_err(|e| warn!("Failed to parse bytecode cache (bincode): {}", e))
            .ok()
    }

    /// Save bytecode cache to disk (binary format).
    pub(crate) fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let data = bincode::serialize(self)?;
        std::fs::write(path, data)?;
        Ok(())
    }

    /// Merge new bytecodes from a BlockchainDb.
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
