//! Generalized storage prefetch registry for EVM cache pre-warming.
//!
//! Captures access lists from EVM interactions (multicall batches, simulations)
//! and persists them across cycles. On the next cycle, batch-fetches the recorded
//! slots into BlockchainDb before the EVM touches them, converting N individual
//! `eth_getStorageAt` RPC calls into a small number of batched HTTP requests
//! (the batch size is governed by the cache's speed mode).
//!
//! Supports two storage shapes:
//! - **Aggregated phases** (e.g., a `pool_refresh` phase): one access list per
//!   phase.
//! - **Keyed phases**: per-address access lists, enabling selective prefetch
//!   for only the addresses that will be simulated.

use std::collections::{HashMap, HashSet};
use std::path::Path;

use alloy_primitives::{Address, U256};
use anyhow::{Context as _, Result};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::StorageAccessList;
use crate::cache::EvmCache;

/// Registry of access lists keyed by phase, persisted across cycles via bincode.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct PrefetchRegistry {
    /// Phases with a single aggregated access list (e.g., a `pool_refresh` phase).
    phases: HashMap<String, StorageAccessList>,
    /// Phases with per-address access lists.
    /// Stored by address so callers can selectively prefetch only ready targets.
    keyed_phases: HashMap<String, HashMap<Address, StorageAccessList>>,
}

impl PrefetchRegistry {
    /// Load a registry from `path` (bincode format).
    ///
    /// Returns [`Default`] (an empty registry) on any error — a missing file, an
    /// unreadable file, or corrupt/undecodable contents. These cases are not
    /// distinguished by the return value: a corrupt registry is indistinguishable
    /// from a fresh start, so a decode failure silently discards previously
    /// persisted prefetch data (logged at `warn`).
    pub fn load(path: &Path) -> Self {
        match std::fs::read(path) {
            Ok(data) => match bincode::deserialize::<PrefetchRegistry>(&data) {
                Ok(registry) => {
                    let phase_count = registry.phases.len();
                    let keyed_phase_count = registry.keyed_phases.len();
                    let total_slots: usize = registry
                        .phases
                        .values()
                        .map(|al| al.slots.len())
                        .sum::<usize>()
                        + registry
                            .keyed_phases
                            .values()
                            .flat_map(|m| m.values())
                            .map(|al| al.slots.len())
                            .sum::<usize>();
                    info!(
                        phases = phase_count,
                        keyed_phases = keyed_phase_count,
                        total_slots,
                        "Loaded prefetch registry"
                    );
                    registry
                }
                Err(e) => {
                    warn!(?e, "Failed to decode prefetch registry, starting fresh");
                    Self::default()
                }
            },
            Err(_) => {
                debug!("No prefetch registry file found, starting fresh");
                Self::default()
            }
        }
    }

    /// Persist the registry to `path` in bincode format, creating parent
    /// directories as needed.
    ///
    /// Returns an error if the parent directory cannot be created, serialization
    /// fails, or the write fails.
    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!("failed to create prefetch registry directory {parent:?}")
            })?;
        }
        let data = bincode::serialize(self).context("failed to serialize prefetch registry")?;
        std::fs::write(path, data)
            .with_context(|| format!("failed to persist prefetch registry to {path:?}"))?;

        let total_slots: usize = self.phases.values().map(|al| al.slots.len()).sum::<usize>()
            + self
                .keyed_phases
                .values()
                .flat_map(|m| m.values())
                .map(|al| al.slots.len())
                .sum::<usize>();
        debug!(total_slots, "Saved prefetch registry");
        Ok(())
    }

    /// Record the aggregated access list for `phase`, **overwriting** any access
    /// list previously recorded for that phase.
    ///
    /// Each call wholesale replaces the phase's slot set; it does not merge with
    /// the prior list. To accumulate per-address lists instead, use
    /// [`record_keyed`](Self::record_keyed).
    ///
    /// ```
    /// use evm_fork_cache::prefetch_registry::PrefetchRegistry;
    /// use evm_fork_cache::StorageAccessList;
    /// use alloy_primitives::{Address, U256};
    ///
    /// let mut registry = PrefetchRegistry::default();
    /// let addr = Address::repeat_byte(0x01);
    ///
    /// let mut al = StorageAccessList::default();
    /// al.slots.insert((addr, U256::from(1)));
    /// registry.record("pool_refresh", al);
    /// assert!(registry.phase_slots("pool_refresh").contains(&(addr, U256::from(1))));
    ///
    /// // A second record replaces the slot set rather than merging.
    /// let mut al2 = StorageAccessList::default();
    /// al2.slots.insert((addr, U256::from(2)));
    /// registry.record("pool_refresh", al2);
    /// let slots = registry.phase_slots("pool_refresh");
    /// assert_eq!(slots.len(), 1);
    /// assert!(slots.contains(&(addr, U256::from(2))));
    /// ```
    pub fn record(&mut self, phase: &str, access_list: StorageAccessList) {
        self.phases.insert(phase.to_string(), access_list);
    }

    /// Record the access list for a single `key` within a keyed `phase`.
    ///
    /// Unlike [`record`](Self::record), this **inserts into** the phase's per-key
    /// nested map: other keys already recorded under `phase` are preserved, and
    /// only the entry for `key` is replaced. Pairs with
    /// [`prefetch_keyed`](Self::prefetch_keyed).
    pub fn record_keyed(&mut self, phase: &str, key: Address, access_list: StorageAccessList) {
        self.keyed_phases
            .entry(phase.to_string())
            .or_default()
            .insert(key, access_list);
    }

    /// Prefetch all slots for an aggregated phase.
    ///
    /// Returns `(fetched, errors)`.
    pub fn prefetch_phase(&self, phase: &str, cache: &mut EvmCache) -> (usize, usize) {
        let Some(access_list) = self.phases.get(phase) else {
            debug!(phase, "No prefetch data for phase");
            return (0, 0);
        };

        if access_list.slots.is_empty() {
            return (0, 0);
        }

        batch_prefetch(cache, access_list.slots.iter().copied(), phase)
    }

    /// Prefetch slots for specific keys within a keyed phase, excluding slots
    /// already warm from a previous prefetch stage.
    ///
    /// Pairs with [`record_keyed`](Self::record_keyed).
    ///
    /// Returns `(fetched, errors)`.
    pub fn prefetch_keyed(
        &self,
        phase: &str,
        keys: &[Address],
        cache: &mut EvmCache,
        exclude: &HashSet<(Address, U256)>,
    ) -> (usize, usize) {
        let Some(keyed_map) = self.keyed_phases.get(phase) else {
            debug!(phase, "No keyed prefetch data for phase");
            return (0, 0);
        };

        let slots: HashSet<(Address, U256)> = keys
            .iter()
            .filter_map(|addr| keyed_map.get(addr))
            .flat_map(|al| al.slots.iter().copied())
            .filter(|slot| !exclude.contains(slot))
            .collect();

        if slots.is_empty() {
            debug!(
                phase,
                keys = keys.len(),
                excluded = exclude.len(),
                "All keyed slots excluded or empty"
            );
            return (0, 0);
        }

        batch_prefetch(cache, slots.into_iter(), phase)
    }

    /// Returns the set of `(address, slot)` pairs recorded for an aggregated
    /// `phase`, or an empty set if the phase was never [`record`](Self::record)ed.
    ///
    /// Typically used to build the `exclude` set passed to
    /// [`prefetch_keyed`](Self::prefetch_keyed) so a later stage skips slots a
    /// prior aggregated prefetch already warmed.
    pub fn phase_slots(&self, phase: &str) -> HashSet<(Address, U256)> {
        self.phases
            .get(phase)
            .map(|al| al.slots.clone())
            .unwrap_or_default()
    }
}

/// Batch-fetch `slots` into `cache` via its `storage_batch_fetcher` and inject
/// the results, returning `(fetched, errors)`.
///
/// Deduplicating, exclusion, and phase lookup are the caller's responsibility
/// ([`PrefetchRegistry::prefetch_phase`] / [`PrefetchRegistry::prefetch_keyed`]).
/// If `slots` is empty, or the cache has no batch fetcher configured, returns
/// `(0, 0)` without fetching. Otherwise each slot that the fetcher resolves
/// successfully is injected into the cache and counted in `fetched`; per-slot
/// fetch errors are counted in `errors` and skipped.
fn batch_prefetch(
    cache: &mut EvmCache,
    slots: impl Iterator<Item = (Address, U256)>,
    phase: &str,
) -> (usize, usize) {
    let requests: Vec<(Address, U256)> = slots.collect();
    if requests.is_empty() {
        return (0, 0);
    }

    let fetcher = match cache.storage_batch_fetcher().cloned() {
        Some(f) => f,
        None => {
            debug!(
                "No batch fetcher available, skipping prefetch for {}",
                phase
            );
            return (0, 0);
        }
    };

    let start = std::time::Instant::now();
    let total_requested = requests.len();
    // `None`: fetch at the cache's currently-pinned block (synchronous, no repin race).
    let results = fetcher(requests, None);

    let mut successes: Vec<(Address, U256, U256)> = Vec::with_capacity(results.len());
    let mut errors = 0usize;
    for (addr, slot, result) in results {
        match result {
            Ok(value) => successes.push((addr, slot, value)),
            Err(_) => errors += 1,
        }
    }

    let fetched = successes.len();
    cache.inject_storage_batch(&successes);

    let ms = start.elapsed().as_millis() as u64;
    info!(
        phase,
        slots_requested = total_requested,
        slots_fetched = fetched,
        errors,
        prefetch_ms = ms,
        "Prefetch complete"
    );

    (fetched, errors)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registry_record_and_phase_slots() {
        let mut registry = PrefetchRegistry::default();
        let mut al = StorageAccessList::default();
        let addr = Address::repeat_byte(0x01);
        al.slots.insert((addr, U256::from(1)));
        al.slots.insert((addr, U256::from(2)));

        registry.record("pool_refresh", al);

        let slots = registry.phase_slots("pool_refresh");
        assert_eq!(slots.len(), 2);
        assert!(slots.contains(&(addr, U256::from(1))));
        assert!(slots.contains(&(addr, U256::from(2))));

        // Non-existent phase returns empty
        assert!(registry.phase_slots("nonexistent").is_empty());
    }

    #[test]
    fn test_registry_record_keyed() {
        let mut registry = PrefetchRegistry::default();
        let key_a = Address::repeat_byte(0x01);
        let key_b = Address::repeat_byte(0x02);

        let mut al1 = StorageAccessList::default();
        al1.slots.insert((key_a, U256::from(10)));

        let mut al2 = StorageAccessList::default();
        al2.slots.insert((key_b, U256::from(20)));

        registry.record_keyed("per_target", key_a, al1);
        registry.record_keyed("per_target", key_b, al2);

        // Verify keyed_phases has both
        let map = registry.keyed_phases.get("per_target").unwrap();
        assert_eq!(map.len(), 2);
        assert!(
            map.get(&key_a)
                .unwrap()
                .slots
                .contains(&(key_a, U256::from(10)))
        );
    }

    #[test]
    fn test_save_load_round_trip() {
        let dir = std::env::temp_dir().join("evm_fork_cache_test_prefetch_registry");
        let path = dir.join("test_registry.bin");
        let _ = std::fs::remove_file(&path);

        let mut registry = PrefetchRegistry::default();
        let addr = Address::repeat_byte(0xAA);
        let mut al = StorageAccessList::default();
        al.slots.insert((addr, U256::from(42)));
        al.accounts.insert(addr);
        registry.record("test_phase", al);

        let key = Address::repeat_byte(0xBB);
        let mut sal = StorageAccessList::default();
        sal.slots.insert((key, U256::from(99)));
        registry.record_keyed("per_target", key, sal);

        registry.save(&path).expect("save registry");

        let loaded = PrefetchRegistry::load(&path);
        assert_eq!(loaded.phases.len(), 1);
        assert!(
            loaded.phases["test_phase"]
                .slots
                .contains(&(addr, U256::from(42)))
        );
        assert_eq!(loaded.keyed_phases.len(), 1);
        assert!(
            loaded.keyed_phases["per_target"]
                .get(&key)
                .unwrap()
                .slots
                .contains(&(key, U256::from(99)))
        );

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir(&dir);
    }

    #[test]
    fn save_reports_write_failures() {
        let dir = std::env::temp_dir().join("evm_fork_cache_test_prefetch_registry_write_error");
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_file(&dir);
        std::fs::write(&dir, b"not a directory").expect("create file path conflict");

        let registry = PrefetchRegistry::default();
        let path = dir.join("registry.bin");
        let err = registry
            .save(&path)
            .expect_err("save must report write failure");
        assert!(
            err.to_string().contains("directory") || err.to_string().contains("Not a directory"),
            "unexpected error: {err:#}"
        );

        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn test_load_missing_file_returns_default() {
        let path = std::path::Path::new("/tmp/nonexistent_prefetch_registry.bin");
        let registry = PrefetchRegistry::load(path);
        assert!(registry.phases.is_empty());
        assert!(registry.keyed_phases.is_empty());
    }

    #[test]
    fn test_record_replaces_existing() {
        let mut registry = PrefetchRegistry::default();
        let addr = Address::repeat_byte(0x01);

        let mut al1 = StorageAccessList::default();
        al1.slots.insert((addr, U256::from(1)));
        registry.record("phase", al1);

        let mut al2 = StorageAccessList::default();
        al2.slots.insert((addr, U256::from(2)));
        registry.record("phase", al2);

        let slots = registry.phase_slots("phase");
        assert_eq!(slots.len(), 1);
        assert!(slots.contains(&(addr, U256::from(2))));
    }
}
