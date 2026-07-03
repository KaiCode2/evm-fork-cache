//! Cold-start root baseline (`roots.bin`): persist observed storage roots so a
//! restarting process can cheaply detect which tracked accounts changed while
//! it was down.
//!
//! An account's storage-trie root (`storageHash`) is a collision-resistant
//! commitment over *all* of that account's storage, so `root_unchanged ⟹
//! nothing under the account changed`. The baseline compares the on-chain root
//! **across time** — never a locally-reconstructed root against the chain: on
//! restart, probe each tracked account's root now and, where it equals the
//! persisted baseline, the cached tracked slots are provably current and are
//! **not** re-read. Where it diverges (or no baseline exists, or the probe
//! fails), the tracked slots are re-read and the new root adopted.
//!
//! This is a **currency** gate, not a **completeness** gate (spec §6): an
//! unchanged root proves the tracked subset did not change, but it cannot tell
//! you that a slot you *should* have been tracking was missing all along.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use alloy_primitives::{Address, B256, U256};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::cache::versioned;
use crate::cold_start::plan::ColdStartPlan;
use crate::cold_start::planner::{ColdStartPlanner, ColdStartStep};
use crate::cold_start::results::ColdStartResults;
use crate::errors::PersistenceError;
use crate::events::StateView;

const ROOT_BASELINE_MAGIC: &[u8; 8] = b"EFCROOT\0";
const ROOT_BASELINE_VERSION: u32 = 1;

/// Serialized payload of a [`RootBaseline`]: sorted `(address, root)` pairs.
///
/// A `Vec` of pairs sorted by address (the [`BTreeMap`] iteration order), so the
/// on-disk bytes are deterministic for a given set of entries.
#[derive(Serialize, Deserialize)]
struct RootBaselineFile {
    roots: Vec<(Address, B256)>,
}

/// A persisted map of `Address -> B256` storage roots — each tracked account's
/// last **observed** on-chain storage root (`storageHash` from `eth_getProof`).
///
/// Persisted via [`save`](Self::save) / [`load`](Self::load) using the same
/// versioned envelope as the binary state cache (magic bytes + version + bincode
/// payload); an unknown magic, unknown version, or corrupt payload is a cache
/// miss (`None`), never an error. The persistence location is the caller's
/// choice — conventionally `roots.bin` alongside the binary state file
/// (`evm_state.bin`).
///
/// # Currency, not completeness
///
/// The baseline is compared **across time** (the root observed now vs. the root
/// observed at the last run), never local-vs-chain. `root_unchanged` proves the
/// tracked subset did not change since the baseline block — it cannot detect
/// that a slot you should have tracked was missing (spec §6).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RootBaseline {
    roots: BTreeMap<Address, B256>,
}

impl RootBaseline {
    /// Record `root` as the observed storage root of `address`, returning the
    /// previously recorded root (if any).
    pub fn insert(&mut self, address: Address, root: B256) -> Option<B256> {
        self.roots.insert(address, root)
    }

    /// The recorded storage root of `address`, if one was observed.
    pub fn get(&self, address: &Address) -> Option<B256> {
        self.roots.get(address).copied()
    }

    /// Number of recorded `(address, root)` entries.
    pub fn len(&self) -> usize {
        self.roots.len()
    }

    /// `true` when no roots are recorded.
    pub fn is_empty(&self) -> bool {
        self.roots.is_empty()
    }

    /// Persist the baseline to `path` (conventionally `roots.bin` next to
    /// `evm_state.bin`).
    ///
    /// The on-disk format carries magic bytes and a version number before the
    /// bincode payload, matching the binary state cache envelope. Entries are
    /// written in address order, so the bytes are deterministic. Returns an
    /// error if serialization, parent-directory creation, or writing fails.
    pub fn save(&self, path: &Path) -> Result<(), PersistenceError> {
        let file = RootBaselineFile {
            roots: self.roots.iter().map(|(a, r)| (*a, *r)).collect(),
        };
        let data = versioned::encode(
            ROOT_BASELINE_MAGIC,
            ROOT_BASELINE_VERSION,
            &file,
            "root baseline",
        )?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|err| PersistenceError::create_dir(parent, err))?;
        }
        std::fs::write(path, &data).map_err(|err| PersistenceError::write(path, err))?;
        debug!(
            entries = self.roots.len(),
            bytes = data.len(),
            "Saved root baseline"
        );
        Ok(())
    }

    /// Load a baseline from `path`.
    ///
    /// Returns `None` (rather than erroring) for a missing file, an unreadable
    /// file, an unknown magic header, an unknown version, or a corrupt payload —
    /// all are cache misses, matching the binary state cache's handling. A
    /// missing file (the normal first-run case) is logged at `debug`; a read
    /// error and any magic/version/decode failure are logged at `warn`.
    pub fn load(path: &Path) -> Option<Self> {
        let data = match std::fs::read(path) {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                debug!("No root baseline file found, starting fresh");
                return None;
            }
            Err(e) => {
                warn!(error = %e, "Failed to read root baseline, starting fresh");
                return None;
            }
        };

        let file = versioned::decode::<RootBaselineFile>(
            &data,
            ROOT_BASELINE_MAGIC,
            ROOT_BASELINE_VERSION,
            "root baseline",
        )?;

        let baseline = Self {
            roots: file.roots.into_iter().collect(),
        };
        debug!(
            entries = baseline.roots.len(),
            bytes = data.len(),
            "Loaded root baseline"
        );
        Some(baseline)
    }
}

/// The outcome of one declared probe-roots address: the storage root the
/// account-proof fetcher observed at the pinned block, or `None` when it
/// could not be observed.
#[derive(Clone, Debug)]
pub struct RootProbeOutcome {
    /// The probed account.
    pub address: Address,
    /// The observed storage root; `None` when the probe failed or the fetcher
    /// omitted the address (treat as unknown -> conservative re-read).
    pub root: Option<B256>,
}

/// Which round the [`RootBaselinePlanner`] is in.
#[derive(Clone, Copy, Debug)]
enum RootBaselinePhase {
    /// Round 1: probe every tracked account's root.
    Probe,
    /// Round 2 (if needed): re-read the tracked slots of diverged/unknown accounts.
    Verify,
}

/// A restart planner that root-gates the tracked working set (Phase-8 §5.5).
///
/// Round 1 probes every tracked account's storage root via the account-proof
/// fetcher (no reads are injected). For each tracked account:
///
/// - observed root **equal** to the baseline ⇒ the cached tracked slots are
///   provably current — the root is retained in the updated baseline and the
///   slots are **not** re-read;
/// - observed root **diverged** (or no baseline entry) ⇒ the new root is
///   adopted into the updated baseline and the account's tracked slots are
///   re-read in a second verify round;
/// - probe **failed** (`root: None`) ⇒ conservative: the tracked slots are
///   re-read, and no root is adopted — an unobserved root never clobbers a
///   previously persisted one.
///
/// When nothing needs re-reading the run finishes after the probe round.
/// [`updated_baseline`](Self::updated_baseline) returns the baseline with this
/// run's adoptions applied — persist it as the next `roots.bin`.
///
/// Like [`RootBaseline`], this is a **currency** gate, not a completeness gate
/// (spec §6): it detects change in the tracked subset, not slots that were
/// never tracked.
pub struct RootBaselinePlanner {
    /// Tracked accounts and the storage slots kept live for each.
    tracked: Vec<(Address, Vec<U256>)>,
    /// The persisted baseline this run compares against (never mutated).
    baseline: RootBaseline,
    /// Baseline-with-adoptions: starts as a copy of `baseline`, updated with
    /// every root observed by this run.
    updated: RootBaseline,
    /// Which round the planner is in.
    phase: RootBaselinePhase,
}

impl RootBaselinePlanner {
    /// Build a planner over `tracked` (`account -> its tracked slots`) and
    /// `baseline` loaded from `roots.bin` (empty when nothing was persisted).
    pub fn new(tracked: Vec<(Address, Vec<U256>)>, baseline: RootBaseline) -> Self {
        let updated = baseline.clone();
        Self {
            tracked,
            baseline,
            updated,
            phase: RootBaselinePhase::Probe,
        }
    }

    /// The observed roots adopted by this run, layered over the loaded baseline
    /// (persist as the next `roots.bin`).
    ///
    /// Accounts whose probe failed keep their previous baseline entry (if any);
    /// accounts whose root was observed carry the observed root.
    pub fn updated_baseline(&self) -> RootBaseline {
        self.updated.clone()
    }
}

impl ColdStartPlanner for RootBaselinePlanner {
    fn initial_plan(&mut self, _state: &dyn StateView) -> ColdStartPlan {
        ColdStartPlan {
            probe_roots: self.tracked.iter().map(|&(address, _)| address).collect(),
            ..Default::default()
        }
    }

    fn on_results(&mut self, results: &ColdStartResults, _state: &dyn StateView) -> ColdStartStep {
        match self.phase {
            RootBaselinePhase::Probe => {
                // Index the probe outcomes; an address absent from the results
                // entirely is treated the same as a failed probe (unknown).
                let observed: HashMap<Address, Option<B256>> = results
                    .probed_roots
                    .iter()
                    .map(|o| (o.address, o.root))
                    .collect();

                let mut verify: Vec<(Address, U256)> = Vec::new();
                for (address, slots) in &self.tracked {
                    match observed.get(address).copied().flatten() {
                        Some(root) => {
                            let current = self.baseline.get(address) == Some(root);
                            // Adopt the observed root either way; skip the
                            // re-read only when it matches the baseline.
                            self.updated.insert(*address, root);
                            if !current {
                                verify.extend(slots.iter().map(|&slot| (*address, slot)));
                            }
                        }
                        // Probe failed / omitted: conservative re-read, and do
                        // NOT adopt a root — `updated` keeps the old baseline
                        // entry (if any) rather than clobbering it.
                        None => verify.extend(slots.iter().map(|&slot| (*address, slot))),
                    }
                }

                if verify.is_empty() {
                    return ColdStartStep::Done;
                }
                self.phase = RootBaselinePhase::Verify;
                ColdStartStep::Continue(ColdStartPlan {
                    verify,
                    ..Default::default()
                })
            }
            RootBaselinePhase::Verify => ColdStartStep::Done,
        }
    }
}
