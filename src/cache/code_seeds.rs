//! Code-seed marks: provenance + trust state for bytecode that did **not**
//! arrive via the lazy RPC backend.
//!
//! Adapters can push runtime code into the cache instead of paying an
//! `eth_getCode` per address (see `EvmCache::seed_account_code` /
//! `EvmCache::etch_account_code`). Every such write records a
//! [`CodeSeedState`] mark; the *absence* of a mark means the code is
//! RPC-origin (fetched from the provider and trusted as chain state).
//!
//! Marks persist across restarts in `code_seeds.bin` so a `Pending` claim can
//! never masquerade as chain-fetched after a reload. The file is written as a
//! crate-specific versioned envelope followed by bincode payload, so
//! incompatible versions are detected as cache misses. Unlike `bytecodes.bin`
//! (load-merge-save, correct for immutable code), this file is saved as a
//! **full replace** of the in-memory map: marks are mutable trust state, and
//! a merge would resurrect marks that were purged this session.

use std::collections::HashMap;
use std::path::Path;

use alloy_primitives::{Address, B256};
use serde::{Deserialize, Serialize};

use super::versioned;
use crate::errors::PersistenceError;

const CODE_SEED_CACHE_MAGIC: &[u8; 8] = b"EFCSEED\0";
const CODE_SEED_CACHE_VERSION: u32 = 1;

/// Provenance + trust state of an address's cached bytecode, for code that
/// did **not** arrive via the lazy RPC backend.
///
/// Absence of a mark means RPC-origin (fetched from the provider, trusted as
/// chain state). See the two write primitives:
/// [`seed_account_code`](crate::cache::EvmCache::seed_account_code) (canonical
/// claim, verified once) and
/// [`etch_account_code`](crate::cache::EvmCache::etch_account_code)
/// (deliberate local divergence).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CodeSeedState {
    /// Canonical claim awaiting on-chain code-hash verification
    /// (`EvmCache::verify_code_seeds`).
    Pending {
        /// keccak256 of the seeded runtime code.
        code_hash: B256,
    },
    /// Canonical claim confirmed against the chain. Never re-verified: post
    /// EIP-6780, deployed code is immutable, so one confirmation is durable.
    /// On chains without 6780 the escape hatch is
    /// [`purge_account`](crate::cache::EvmCache::purge_account), which clears
    /// the mark.
    Verified {
        /// keccak256 of the verified runtime code.
        code_hash: B256,
        /// Pinned block number at which the on-chain code hash matched.
        verified_at_block: u64,
    },
    /// Deliberate local divergence (an unreleased contract, a test harness).
    /// Never verified, excluded from all canonical machinery, and reported on
    /// the health surface via
    /// [`etched_accounts`](crate::cache::EvmCache::etched_accounts).
    Etched {
        /// keccak256 of the etched runtime code.
        code_hash: B256,
    },
}

impl CodeSeedState {
    /// The keccak256 code hash this mark refers to.
    pub fn code_hash(&self) -> B256 {
        match self {
            Self::Pending { code_hash }
            | Self::Verified { code_hash, .. }
            | Self::Etched { code_hash } => *code_hash,
        }
    }
}

/// Serializable code-seed mark store (`code_seeds.bin`).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct CodeSeedCache {
    /// Map of address to its code-seed mark.
    pub(crate) entries: HashMap<Address, CodeSeedState>,
}

impl CodeSeedCache {
    /// Load the mark store from disk (binary format).
    ///
    /// Returns `None` if `path` cannot be read, fails the magic/version check,
    /// or fails to decode as bincode for this type — legacy/missing files are
    /// cache misses, never errors.
    pub(crate) fn load(path: &Path) -> Option<Self> {
        let data = std::fs::read(path).ok()?;
        versioned::decode(
            &data,
            CODE_SEED_CACHE_MAGIC,
            CODE_SEED_CACHE_VERSION,
            "code seed cache",
        )
    }

    /// Save the mark store to disk (binary format), replacing any previous
    /// file wholesale (see the module docs for why this is not a merge).
    ///
    /// # Errors
    ///
    /// Returns an error if the parent directory cannot be created, if bincode
    /// serialization fails, or if writing the file fails.
    pub(crate) fn save(&self, path: &Path) -> Result<(), PersistenceError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|err| PersistenceError::create_dir(parent, err))?;
        }
        let data = versioned::encode(
            CODE_SEED_CACHE_MAGIC,
            CODE_SEED_CACHE_VERSION,
            self,
            "code seed cache",
        )?;
        std::fs::write(path, data).map_err(|err| PersistenceError::write(path, err))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_path(tag: &str) -> std::path::PathBuf {
        // Keyed by pid so concurrent `cargo test` processes never share (and
        // never `remove_dir_all`) each other's directory.
        let dir = std::env::temp_dir().join(format!(
            "evm_fork_cache_code_seeds_{tag}_{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create temp dir");
        dir.join("code_seeds.bin")
    }

    #[test]
    fn save_load_round_trip_preserves_all_three_marks() {
        let path = temp_path("roundtrip");
        let pending = Address::repeat_byte(0x01);
        let verified = Address::repeat_byte(0x02);
        let etched = Address::repeat_byte(0x03);

        let mut cache = CodeSeedCache::default();
        cache.entries.insert(
            pending,
            CodeSeedState::Pending {
                code_hash: B256::repeat_byte(0xaa),
            },
        );
        cache.entries.insert(
            verified,
            CodeSeedState::Verified {
                code_hash: B256::repeat_byte(0xbb),
                verified_at_block: 123,
            },
        );
        cache.entries.insert(
            etched,
            CodeSeedState::Etched {
                code_hash: B256::repeat_byte(0xcc),
            },
        );
        cache.save(&path).expect("save code seed cache");

        let bytes = std::fs::read(&path).expect("read saved code seed cache");
        assert!(
            bytes.starts_with(b"EFCSEED\0"),
            "code seed cache must carry a magic header"
        );
        assert_eq!(
            &bytes[8..12],
            &1u32.to_le_bytes(),
            "code seed cache must carry an explicit version"
        );

        let loaded = CodeSeedCache::load(&path).expect("load code seed cache");
        assert_eq!(loaded.entries.len(), 3);
        assert_eq!(
            loaded.entries.get(&pending),
            Some(&CodeSeedState::Pending {
                code_hash: B256::repeat_byte(0xaa)
            }),
            "Pending survives a reload as Pending — it must never masquerade as RPC-origin"
        );
        assert_eq!(
            loaded.entries.get(&verified),
            Some(&CodeSeedState::Verified {
                code_hash: B256::repeat_byte(0xbb),
                verified_at_block: 123
            })
        );
        assert_eq!(
            loaded.entries.get(&etched),
            Some(&CodeSeedState::Etched {
                code_hash: B256::repeat_byte(0xcc)
            })
        );

        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn load_unversioned_or_missing_is_none() {
        let path = temp_path("legacy");
        let mut cache = CodeSeedCache::default();
        cache.entries.insert(
            Address::repeat_byte(0x42),
            CodeSeedState::Etched {
                code_hash: B256::repeat_byte(0x42),
            },
        );
        std::fs::write(&path, bincode::serialize(&cache).unwrap()).expect("write legacy cache");
        assert!(
            CodeSeedCache::load(&path).is_none(),
            "unversioned legacy bincode must be treated as a cache miss"
        );
        assert!(CodeSeedCache::load(std::path::Path::new("/nonexistent/code_seeds.bin")).is_none());

        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn save_is_full_replace_not_merge() {
        let path = temp_path("replace");
        let stale = Address::repeat_byte(0x01);
        let kept = Address::repeat_byte(0x02);

        let mut first = CodeSeedCache::default();
        first.entries.insert(
            stale,
            CodeSeedState::Pending {
                code_hash: B256::repeat_byte(0xaa),
            },
        );
        first.save(&path).expect("save first");

        // A second save without the stale entry must not resurrect it: a
        // purged mark staying purged is the whole point of replace semantics.
        let mut second = CodeSeedCache::default();
        second.entries.insert(
            kept,
            CodeSeedState::Etched {
                code_hash: B256::repeat_byte(0xbb),
            },
        );
        second.save(&path).expect("save second");

        let loaded = CodeSeedCache::load(&path).expect("load code seed cache");
        assert_eq!(loaded.entries.len(), 1, "stale entry must not survive");
        assert!(loaded.entries.contains_key(&kept));
        assert!(!loaded.entries.contains_key(&stale));

        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }
}
