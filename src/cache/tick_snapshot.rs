//! Persisted snapshots of UniswapV3-style tick state.
//!
//! Loading every initialized tick of a concentrated-liquidity pool over RPC is
//! expensive, so this module defines the public per-tick state ([`TickInfo`]),
//! its serializable on-disk counterpart ([`SerializableTickInfo`]), and the
//! snapshot containers used to persist a pool's tick data to disk and reload it
//! on a later run, avoiding repeated tick scans.

use std::collections::HashMap;
use std::path::Path;

use alloy_primitives::{Address, U256};
use anyhow::Result;
use serde::{Deserialize, Serialize};

use super::versioned;

const TICK_SNAPSHOT_CACHE_MAGIC: &[u8; 8] = b"EFCTICK\0";
const TICK_SNAPSHOT_CACHE_VERSION: u32 = 1;

/// Per-tick liquidity state for a UniswapV3-style concentrated-liquidity pool.
///
/// This is the public, dependency-free representation of a single tick's
/// `Tick.Info` used by [`crate::cache::EvmCache::inject_v3_ticks`] and returned by
/// [`V3PoolTickSnapshot::to_ticks`]. It mirrors the three fields of the
/// on-chain struct that matter for swap simulation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TickInfo {
    /// Total liquidity that references this tick (`liquidityGross`).
    pub liquidity_gross: u128,
    /// Net liquidity added/removed when the tick is crossed (`liquidityNet`).
    pub liquidity_net: i128,
    /// Whether the tick is initialized; controls whether it is processed
    /// during swap execution.
    pub initialized: bool,
}

/// Serializable tick info for V3 pools.
///
/// On-disk counterpart of [`TickInfo`] with the same three fields. It exists as
/// a distinct type so the persisted snapshot format can evolve independently of
/// the public [`TickInfo`] used by the simulation API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializableTickInfo {
    pub liquidity_gross: u128,
    pub liquidity_net: i128,
    pub initialized: bool,
}

/// Cached tick data snapshot for a UniswapV3 pool.
///
/// This captures the tick_bitmap and tick Info at a point in time,
/// allowing us to skip expensive tick re-scanning on restart if the
/// pool state hasn't changed significantly.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct V3PoolTickSnapshot {
    /// Tick bitmap: word position -> bitmap value
    /// Using String keys for JSON serialization (i16 keys not directly supported)
    pub tick_bitmap: HashMap<String, U256>,
    /// Tick info: tick index -> (liquidity_gross, liquidity_net, initialized)
    /// Using String keys for JSON serialization (i32 keys not directly supported)
    pub ticks: HashMap<String, SerializableTickInfo>,
    /// Global liquidity at snapshot time (used for cache validation)
    pub last_liquidity: u128,
    /// Tick at snapshot time
    pub last_tick: i32,
}

impl V3PoolTickSnapshot {
    /// Create a new tick snapshot from pool data.
    ///
    /// Captures the in-memory `tick_bitmap` and `ticks` maps along with the
    /// pool's current `liquidity` and `tick`, converting the integer map keys to
    /// their `String` form for serialization. The conversion is total (no entry
    /// is dropped); the inverse [`V3PoolTickSnapshot::to_tick_bitmap`] /
    /// [`V3PoolTickSnapshot::to_ticks`] may drop entries whose string keys fail
    /// to parse.
    pub fn from_pool_data(
        tick_bitmap: &std::collections::HashMap<i16, U256>,
        ticks: &std::collections::HashMap<i32, TickInfo>,
        liquidity: u128,
        tick: i32,
    ) -> Self {
        Self {
            tick_bitmap: tick_bitmap
                .iter()
                .map(|(k, v)| (k.to_string(), *v))
                .collect(),
            ticks: ticks
                .iter()
                .map(|(k, v)| {
                    (
                        k.to_string(),
                        SerializableTickInfo {
                            liquidity_gross: v.liquidity_gross,
                            liquidity_net: v.liquidity_net,
                            initialized: v.initialized,
                        },
                    )
                })
                .collect(),
            last_liquidity: liquidity,
            last_tick: tick,
        }
    }

    /// Convert tick_bitmap back to HashMap<i16, U256>.
    ///
    /// Reverses the `i16 -> String` keying done by
    /// [`V3PoolTickSnapshot::from_pool_data`]. Any entry whose string key does
    /// not parse back to an `i16` is silently dropped, so a corrupted or
    /// out-of-range key produces a smaller map rather than an error.
    pub fn to_tick_bitmap(&self) -> std::collections::HashMap<i16, U256> {
        self.tick_bitmap
            .iter()
            .filter_map(|(k, v)| k.parse::<i16>().ok().map(|key| (key, *v)))
            .collect()
    }

    /// Convert ticks back to `HashMap<i32, TickInfo>`.
    ///
    /// Reverses the `i32 -> String` keying done by
    /// [`V3PoolTickSnapshot::from_pool_data`]. Any entry whose string key does
    /// not parse back to an `i32` is silently dropped, so a corrupted or
    /// out-of-range key produces a smaller map rather than an error.
    pub fn to_ticks(&self) -> std::collections::HashMap<i32, TickInfo> {
        self.ticks
            .iter()
            .filter_map(|(k, v)| {
                k.parse::<i32>().ok().map(|key| {
                    (
                        key,
                        TickInfo {
                            liquidity_gross: v.liquidity_gross,
                            liquidity_net: v.liquidity_net,
                            initialized: v.initialized,
                        },
                    )
                })
            })
            .collect()
    }
}

/// Cache for V3 pool tick snapshots.
///
/// Stored in a separate file from immutable data since tick data
/// can change (though infrequently) and may be large.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct V3TickSnapshotCache {
    /// Pool address -> tick snapshot
    pub snapshots: HashMap<Address, V3PoolTickSnapshot>,
}

impl V3TickSnapshotCache {
    /// Load tick snapshot cache from disk (binary format).
    ///
    /// Returns `None` if `path` cannot be read, fails the magic/version check, or
    /// fails to decode as bincode for this type.
    pub fn load(path: &Path) -> Option<Self> {
        let data = std::fs::read(path).ok()?;
        versioned::decode(
            &data,
            TICK_SNAPSHOT_CACHE_MAGIC,
            TICK_SNAPSHOT_CACHE_VERSION,
            "V3 tick snapshot cache",
        )
    }

    /// Save tick snapshot cache to disk (binary format).
    ///
    /// Creates the parent directory if needed, then writes the
    /// bincode-serialized cache to `path`.
    ///
    /// # Errors
    ///
    /// Returns an error if the parent directory cannot be created, if bincode
    /// serialization fails, or if writing the file fails.
    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let data = versioned::encode(
            TICK_SNAPSHOT_CACHE_MAGIC,
            TICK_SNAPSHOT_CACHE_VERSION,
            self,
            "V3 tick snapshot cache",
        )?;
        std::fs::write(path, data)?;
        Ok(())
    }

    /// Get a tick snapshot for a pool.
    pub fn get(&self, address: Address) -> Option<&V3PoolTickSnapshot> {
        self.snapshots.get(&address)
    }

    /// Store a tick snapshot for a pool.
    ///
    /// Overwrites any existing snapshot for `address`.
    pub fn set(&mut self, address: Address, snapshot: V3PoolTickSnapshot) {
        self.snapshots.insert(address, snapshot);
    }

    /// Remove a tick snapshot for a pool.
    ///
    /// A no-op if no snapshot is stored for `address`.
    pub fn remove(&mut self, address: Address) {
        self.snapshots.remove(&address);
    }

    /// Get the number of cached snapshots.
    pub fn len(&self) -> usize {
        self.snapshots.len()
    }

    /// Check if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.snapshots.is_empty()
    }
}
