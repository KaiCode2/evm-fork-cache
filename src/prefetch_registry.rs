//! Generalized storage prefetch registry for EVM cache pre-warming.
//!
//! Captures access lists from EVM interactions (multicall batches, simulations)
//! and persists them across cycles. On the next cycle, batch-fetches the recorded
//! slots into BlockchainDb before the EVM touches them, converting N individual
//! `eth_getStorageAt` RPC calls into ⌈N/200⌉ batched HTTP requests.
//!
//! Supports two storage shapes:
//! - **Aggregated phases** (e.g., `cooldown_eval`): one access list per phase.
//! - **Keyed phases**: per-address access lists, enabling selective prefetch
//!   for only the addresses that will be simulated.

use std::collections::{HashMap, HashSet};
use std::path::Path;

use alloy_primitives::{Address, U256};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::StorageAccessList;
use crate::cache::EvmCache;

/// Registry of access lists keyed by phase, persisted across cycles via bincode.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct PrefetchRegistry {
    /// Phases with a single aggregated access list (e.g., cooldown_eval).
    phases: HashMap<String, StorageAccessList>,
    /// Phases with per-address access lists.
    /// Stored by address so callers can selectively prefetch only ready targets.
    strategy_phases: HashMap<String, HashMap<Address, StorageAccessList>>,
}

impl PrefetchRegistry {
    /// Load from disk (bincode format). Returns empty registry if file missing or corrupt.
    pub fn load(path: &Path) -> Self {
        match std::fs::read(path) {
            Ok(data) => match bincode::deserialize::<PrefetchRegistry>(&data) {
                Ok(registry) => {
                    let phase_count = registry.phases.len();
                    let strategy_phase_count = registry.strategy_phases.len();
                    let total_slots: usize = registry
                        .phases
                        .values()
                        .map(|al| al.slots.len())
                        .sum::<usize>()
                        + registry
                            .strategy_phases
                            .values()
                            .flat_map(|m| m.values())
                            .map(|al| al.slots.len())
                            .sum::<usize>();
                    info!(
                        phases = phase_count,
                        strategy_phases = strategy_phase_count,
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
                // Check for legacy harvest_access_lists.json and migrate.
                // Try both the parent directory and the original hardcoded location.
                let candidates = [
                    path.parent().map(|p| p.join("harvest_access_lists.json")),
                    Some(std::path::PathBuf::from("data/harvest_access_lists.json")),
                ];
                for candidate in candidates.into_iter().flatten() {
                    if let Ok(json) = std::fs::read_to_string(&candidate)
                        && let Ok(legacy) =
                            serde_json::from_str::<HashMap<Address, StorageAccessList>>(&json)
                    {
                        info!(
                            strategies = legacy.len(),
                            path = %candidate.display(),
                            "Migrated legacy harvest_access_lists.json to prefetch registry"
                        );
                        let mut registry = Self::default();
                        registry
                            .strategy_phases
                            .insert("harvest_sim".to_string(), legacy);
                        return registry;
                    }
                }
                debug!("No prefetch registry file found, starting fresh");
                Self::default()
            }
        }
    }

    /// Persist to disk (bincode format).
    pub fn save(&self, path: &Path) {
        if let Some(parent) = path.parent()
            && let Err(e) = std::fs::create_dir_all(parent)
        {
            warn!(error = %e, "Failed to create prefetch registry directory");
            return;
        }
        match bincode::serialize(self) {
            Ok(data) => {
                if let Err(e) = std::fs::write(path, data) {
                    warn!(error = %e, "Failed to persist prefetch registry");
                } else {
                    let total_slots: usize =
                        self.phases.values().map(|al| al.slots.len()).sum::<usize>()
                            + self
                                .strategy_phases
                                .values()
                                .flat_map(|m| m.values())
                                .map(|al| al.slots.len())
                                .sum::<usize>();
                    debug!(total_slots, "Saved prefetch registry");
                }
            }
            Err(e) => warn!(error = %e, "Failed to serialize prefetch registry"),
        }
    }

    /// Record an aggregated access list for a phase (replaces any existing).
    pub fn record(&mut self, phase: &str, access_list: StorageAccessList) {
        self.phases.insert(phase.to_string(), access_list);
    }

    /// Record a keyed access list within a phase.
    pub fn record_keyed(&mut self, phase: &str, key: Address, access_list: StorageAccessList) {
        self.strategy_phases
            .entry(phase.to_string())
            .or_default()
            .insert(key, access_list);
    }

    /// Record a per-strategy access list within a phase.
    ///
    /// Kept for compatibility with the existing bot. New generic callers should
    /// prefer [`record_keyed`](Self::record_keyed).
    pub fn record_strategy(
        &mut self,
        phase: &str,
        strategy: Address,
        access_list: StorageAccessList,
    ) {
        self.record_keyed(phase, strategy, access_list);
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

    /// Prefetch slots for specific strategies within a per-strategy phase,
    /// excluding slots already warm from a previous prefetch stage.
    ///
    /// Returns `(fetched, errors)`.
    pub fn prefetch_strategies(
        &self,
        phase: &str,
        strategies: &[Address],
        cache: &mut EvmCache,
        exclude: &HashSet<(Address, U256)>,
    ) -> (usize, usize) {
        let Some(strategy_map) = self.strategy_phases.get(phase) else {
            debug!(phase, "No per-strategy prefetch data for phase");
            return (0, 0);
        };

        let slots: HashSet<(Address, U256)> = strategies
            .iter()
            .filter_map(|addr| strategy_map.get(addr))
            .flat_map(|al| al.slots.iter().copied())
            .filter(|slot| !exclude.contains(slot))
            .collect();

        if slots.is_empty() {
            debug!(
                phase,
                strategies = strategies.len(),
                excluded = exclude.len(),
                "All strategy slots excluded or empty"
            );
            return (0, 0);
        }

        batch_prefetch(cache, slots.into_iter(), phase)
    }

    /// Returns the set of (address, slot) pairs for an aggregated phase.
    /// Used to build exclusion sets for subsequent prefetches.
    pub fn phase_slots(&self, phase: &str) -> HashSet<(Address, U256)> {
        self.phases
            .get(phase)
            .map(|al| al.slots.clone())
            .unwrap_or_default()
    }
}

/// Batch-fetch slots into the EVM cache via `storage_batch_fetcher`.
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
    let results = fetcher(requests);

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

        registry.record("cooldown_eval", al);

        let slots = registry.phase_slots("cooldown_eval");
        assert_eq!(slots.len(), 2);
        assert!(slots.contains(&(addr, U256::from(1))));
        assert!(slots.contains(&(addr, U256::from(2))));

        // Non-existent phase returns empty
        assert!(registry.phase_slots("nonexistent").is_empty());
    }

    #[test]
    fn test_registry_record_strategy() {
        let mut registry = PrefetchRegistry::default();
        let strategy1 = Address::repeat_byte(0x01);
        let strategy2 = Address::repeat_byte(0x02);

        let mut al1 = StorageAccessList::default();
        al1.slots.insert((strategy1, U256::from(10)));

        let mut al2 = StorageAccessList::default();
        al2.slots.insert((strategy2, U256::from(20)));

        registry.record_strategy("harvest_sim", strategy1, al1);
        registry.record_strategy("harvest_sim", strategy2, al2);

        // Verify strategy_phases has both
        let map = registry.strategy_phases.get("harvest_sim").unwrap();
        assert_eq!(map.len(), 2);
        assert!(
            map.get(&strategy1)
                .unwrap()
                .slots
                .contains(&(strategy1, U256::from(10)))
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

        let strategy = Address::repeat_byte(0xBB);
        let mut sal = StorageAccessList::default();
        sal.slots.insert((strategy, U256::from(99)));
        registry.record_strategy("harvest_sim", strategy, sal);

        registry.save(&path);

        let loaded = PrefetchRegistry::load(&path);
        assert_eq!(loaded.phases.len(), 1);
        assert!(
            loaded.phases["test_phase"]
                .slots
                .contains(&(addr, U256::from(42)))
        );
        assert_eq!(loaded.strategy_phases.len(), 1);
        assert!(
            loaded.strategy_phases["harvest_sim"]
                .get(&strategy)
                .unwrap()
                .slots
                .contains(&(strategy, U256::from(99)))
        );

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir(&dir);
    }

    #[test]
    fn test_load_missing_file_returns_default() {
        let path = std::path::Path::new("/tmp/nonexistent_prefetch_registry.bin");
        let registry = PrefetchRegistry::load(path);
        assert!(registry.phases.is_empty());
        assert!(registry.strategy_phases.is_empty());
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
