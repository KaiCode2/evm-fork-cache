//! Trace-based discovery of hash-derived storage slots.
//!
//! Solidity, Vyper, and hand-written assembly contracts all place `mapping`
//! entries (and dynamic arrays) at storage slots derived with `keccak256`. The
//! canonical Solidity layout for `mapping(K => V)` entry `m[k]` is
//! `keccak256(k ‖ slot)`; Vyper hashes the same two words in the opposite order
//! (`keccak256(slot ‖ k)`); Solady-style assembly packs a key and a seed into a
//! single 32-byte word. That diversity makes a *static* base-slot guess
//! unreliable across tokens.
//!
//! This module derives the layout **dynamically** from a single simulated call.
//! [`HashStorageProbe`] is a [`revm::Inspector`] that records, over one
//! execution:
//!   * every `KECCAK256` preimage, keyed by its hash output, and
//!   * every `SLOAD` — its slot and the value it loaded.
//!
//! [`HashStorageProbe::accesses`] then factors each *hashed* `SLOAD` back into a
//! [`HashSlotAccess`] — the mapping key chain, the declared base slot, the
//! detected [`SlotLayout`], and the exact storage slot that was read — **without
//! assuming a preimage byte order**. See [`resolve`](HashStorageProbe::accesses)
//! for the disambiguation rules.
//!
//! A discovered single-level mapping can be captured as a [`TrackedMapping`],
//! a small reusable descriptor whose [`TrackedMapping::slot_for`] recomputes the
//! exact storage slot for *any* key using the discovered layout. That is the
//! building block for "derive a token's balance slot once, then track these
//! addresses" workflows: discover with the probe, then fan out cheaply.
//!
//! # Coupling
//!
//! This module depends only on `revm` and `alloy` primitives — it is decoupled
//! from balances, allowances, or any ERC-20 semantics. [`EvmCache`] builds
//! ergonomic wrappers on top (balance-slot discovery, layout-aware writes); the
//! reactive freshness/prefetch layers consume [`TrackedMapping::slot_for`] to
//! register derived slots.
//!
//! [`EvmCache`]: crate::cache::EvmCache

use std::collections::HashMap;
use std::fmt;

use alloy_primitives::{Address, B256, U256, keccak256};
use revm::Inspector;
use revm::interpreter::interpreter_types::{Jumps, MemoryTr, StackTr};
use revm::interpreter::{Interpreter, InterpreterTypes};

/// `KECCAK256` (a.k.a. `SHA3`) opcode.
const OP_KECCAK256: u8 = 0x20;
/// `SLOAD` opcode.
const OP_SLOAD: u8 = 0x54;
/// Upper bound on a hashed preimage we will record. Mapping/array preimages are
/// 32 or 64 bytes; this guards against hashing large memory regions.
const MAX_PREIMAGE_LEN: usize = 4096;

// ===========================================================================
// Public result types
// ===========================================================================

/// The storage layout a [`HashSlotAccess`] was factored into.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SlotLayout {
    /// `keccak256(key ‖ slot)` — Solidity's `mapping` layout.
    SolidityMapping,
    /// `keccak256(slot ‖ key)` — Vyper's `HashMap` layout (words swapped).
    VyperMapping,
    /// A nested mapping (2+ chained hashes), e.g. an ERC-20 allowance.
    Nested,
    /// `keccak256(addr ‖ seed)` packed into a single 32-byte word
    /// (Solady/assembly), where `seed` is a per-mapping constant.
    PackedSeed {
        /// The packed low-word seed (the mapping's identifier).
        seed: U256,
    },
    /// `keccak256(slot)` — a dynamic-array or base-pointer slot.
    ArrayPointer,
    /// Recognized as hash-derived but not factorable into a known shape.
    Opaque,
}

impl fmt::Display for SlotLayout {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SlotLayout::SolidityMapping => f.write_str("mapping(key‖slot) [Solidity]"),
            SlotLayout::VyperMapping => f.write_str("mapping(slot‖key) [Vyper]"),
            SlotLayout::Nested => f.write_str("nested mapping"),
            SlotLayout::PackedSeed { seed } => write!(f, "packed(addr‖seed={seed:#x}) [Solady]"),
            SlotLayout::ArrayPointer => f.write_str("array/base pointer"),
            SlotLayout::Opaque => f.write_str("opaque"),
        }
    }
}

/// Confidence in a [`HashSlotAccess`] factoring, ordered weakest → strongest so
/// `min` degrades correctly as evidence weakens.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Confidence {
    /// Ambiguous (e.g. both preimage halves look like keys).
    Low,
    /// A 32-byte packed/array fallback with no known-key anchor.
    Heuristic,
    /// Resolved by significant-byte magnitude (a small slot vs a wide key).
    Medium,
    /// A key half matched a caller-supplied known key.
    High,
}

impl fmt::Display for Confidence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

/// One hash-derived `SLOAD` observed during a simulation, factored into its
/// mapping structure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HashSlotAccess {
    /// The exact storage slot that was read — the direct override/track target.
    pub slot: B256,
    /// The value the `SLOAD` returned.
    pub value: U256,
    /// Mapping keys, outer → inner. For `m[a][b]` this is `[b, a]`; empty for a
    /// plain array/base pointer.
    pub keys: Vec<B256>,
    /// The inferred declared base slot (or packed seed) of the outer mapping.
    pub base_slot: U256,
    /// The detected storage layout.
    pub layout: SlotLayout,
    /// Nesting depth (number of hashes in the derivation chain).
    pub depth: usize,
    /// Confidence in the factoring.
    pub confidence: Confidence,
}

impl HashSlotAccess {
    /// True if any key in the chain equals `k` (matched as a full 32-byte word
    /// or as an address in the word's low 20 bytes).
    pub fn keyed_by(&self, k: B256) -> bool {
        self.keys.iter().any(|key| word_matches(*key, k))
    }

    /// Capture a **single-level** mapping access as a reusable [`TrackedMapping`]
    /// on `contract`. Returns `None` for nested, array, or opaque layouts (their
    /// slot cannot be recomputed from a base slot and one key alone).
    pub fn as_tracked(&self, contract: Address) -> Option<TrackedMapping> {
        if self.keys.len() != 1 {
            return None;
        }
        match self.layout {
            SlotLayout::SolidityMapping
            | SlotLayout::VyperMapping
            | SlotLayout::PackedSeed { .. } => Some(TrackedMapping {
                contract,
                base_slot: self.base_slot,
                layout: self.layout,
            }),
            _ => None,
        }
    }
}

/// A reusable descriptor for a single-level hash-derived mapping on one
/// contract, from which the storage slot of any key can be recomputed.
///
/// Obtain one from [`HashSlotAccess::as_tracked`] (or
/// [`EvmCache::discover_erc20_balance_slot`](crate::cache::EvmCache::discover_erc20_balance_slot)),
/// then call [`slot_for`](Self::slot_for) for each key you want to track — no
/// re-simulation required.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TrackedMapping {
    /// The contract whose storage holds the mapping.
    pub contract: Address,
    /// The mapping's declared base slot (or packed seed).
    pub base_slot: U256,
    /// The layout used to derive entry slots.
    pub layout: SlotLayout,
}

impl TrackedMapping {
    /// Build a descriptor explicitly (when the layout is already known).
    pub fn new(contract: Address, base_slot: U256, layout: SlotLayout) -> Self {
        Self {
            contract,
            base_slot,
            layout,
        }
    }

    /// The storage slot of the entry keyed by `key`, using the tracked layout.
    ///
    /// `key` is a full 32-byte word; for an address key pass
    /// [`Address::into_word`]. Returns `None` if the layout is not a
    /// single-level mapping shape.
    pub fn slot_for(&self, key: B256) -> Option<B256> {
        match self.layout {
            SlotLayout::SolidityMapping => {
                let mut pre = [0u8; 64];
                pre[0..32].copy_from_slice(key.as_slice());
                pre[32..64].copy_from_slice(self.base_slot_word().as_slice());
                Some(keccak256(pre))
            }
            SlotLayout::VyperMapping => {
                let mut pre = [0u8; 64];
                pre[0..32].copy_from_slice(self.base_slot_word().as_slice());
                pre[32..64].copy_from_slice(key.as_slice());
                Some(keccak256(pre))
            }
            SlotLayout::PackedSeed { seed } => {
                // Solady packs the address in the high 20 bytes and the seed in
                // the low 12 bytes of a single word.
                let mut pre = [0u8; 32];
                pre[0..20].copy_from_slice(&Address::from_word(key).into_array());
                let seed_be = seed.to_be_bytes::<32>();
                pre[20..32].copy_from_slice(&seed_be[20..32]);
                Some(keccak256(pre))
            }
            _ => None,
        }
    }

    /// Compute `(key, slot)` for each key, skipping any the layout can't derive.
    pub fn slots_for(&self, keys: impl IntoIterator<Item = B256>) -> Vec<(B256, B256)> {
        keys.into_iter()
            .filter_map(|k| self.slot_for(k).map(|s| (k, s)))
            .collect()
    }

    fn base_slot_word(&self) -> B256 {
        B256::from(self.base_slot.to_be_bytes::<32>())
    }
}

/// A discovered balance mapping paired with each tracked holder's
/// `(address, storage slot)` — the return of
/// [`EvmCache::track_erc20_balances`](crate::cache::EvmCache::track_erc20_balances).
pub type TrackedBalances = (TrackedMapping, Vec<(Address, B256)>);

// ===========================================================================
// The inspector
// ===========================================================================

#[derive(Clone, Debug)]
struct SloadRecord {
    slot: B256,
    value: U256,
}

/// A [`revm::Inspector`] that records `KECCAK256` preimages and **every** `SLOAD`
/// (slot + loaded value), so [`accesses`](Self::accesses) can reconstruct the
/// mapping layout of hashed reads and [`slots_returning`](Self::slots_returning)
/// can find the slot that drove a getter's return value (hashed *or* plain).
///
/// Attach it through
/// [`EvmCache::call_raw_with_inspector`](crate::cache::EvmCache::call_raw_with_inspector)
/// or [`EvmOverlay::call_raw_with_inspector`](crate::cache::EvmOverlay::call_raw_with_inspector),
/// and it composes with other inspectors via
/// [`InspectorStack`](crate::InspectorStack) so discovery can piggyback on a
/// simulation you are already running.
///
/// ```
/// use evm_fork_cache::mapping_probe::HashStorageProbe;
/// let probe = HashStorageProbe::new();
/// assert!(probe.accesses(&[]).is_empty()); // nothing executed yet
/// ```
#[derive(Clone, Debug, Default)]
pub struct HashStorageProbe {
    /// `keccak256(preimage) -> preimage`, every hash computed during the call.
    preimages: HashMap<B256, Vec<u8>>,
    /// Every `SLOAD` (slot + loaded value), in observation order.
    reads: Vec<SloadRecord>,
    /// Slot set on a `SLOAD` `step`, resolved to a value in the next `step_end`.
    pending: Option<B256>,
}

impl HashStorageProbe {
    /// Create an empty probe.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of distinct `KECCAK256` preimages observed.
    pub fn preimage_count(&self) -> usize {
        self.preimages.len()
    }

    /// Number of hash-derived `SLOAD`s observed (a subset of all reads).
    pub fn hashed_read_count(&self) -> usize {
        self.reads
            .iter()
            .filter(|r| self.preimages.contains_key(&r.slot))
            .count()
    }

    /// Storage slots whose loaded value equalled `value`, deduplicated in
    /// observation order.
    ///
    /// This is the building block for mocking a getter's return: the slot a view
    /// call read that equals what it returned is (almost always) the slot that
    /// *drives* the return. Includes plain (non-hashed) slots, so it covers
    /// `totalSupply`-style variables as well as mapping entries.
    pub fn slots_returning(&self, value: U256) -> Vec<B256> {
        let mut seen = std::collections::HashSet::new();
        self.reads
            .iter()
            .filter(|r| r.value == value)
            .map(|r| r.slot)
            .filter(|slot| seen.insert(*slot))
            .collect()
    }

    /// Resolve every hash-derived `SLOAD` into a [`HashSlotAccess`].
    ///
    /// `known` are words (e.g. addresses via [`Address::into_word`]) the caller
    /// wants matched as mapping keys; pass `&[]` to rely purely on the
    /// significant-byte magnitude heuristic. Results are in observation order.
    ///
    /// # Disambiguation
    ///
    /// * **64-byte preimage `X ‖ Y`** — a mapping entry. If one half is itself a
    ///   recorded hash it is the parent location (nested mapping) and the other
    ///   is the key; recurse. Otherwise the key is the half matching a `known`
    ///   word, else the half with more significant bytes (a 20-byte address
    ///   dwarfs a small slot index); the remaining half is the base slot.
    /// * **32-byte preimage** — Solady-style `addr ‖ seed` (detected when the
    ///   high 20 bytes match a `known` address) or a `keccak(slot)` array
    ///   pointer.
    pub fn accesses(&self, known: &[B256]) -> Vec<HashSlotAccess> {
        self.reads
            .iter()
            .filter(|r| self.preimages.contains_key(&r.slot))
            .map(|r| resolve(r.slot, r.value, &self.preimages, known))
            .collect()
    }
}

impl<CTX, INTR: InterpreterTypes> Inspector<CTX, INTR> for HashStorageProbe {
    fn step(&mut self, interp: &mut Interpreter<INTR>, _ctx: &mut CTX) {
        match interp.bytecode.opcode() {
            OP_KECCAK256 => {
                // Stack (top first): offset, size.
                let (offset, size) = {
                    let s = interp.stack.data();
                    let n = s.len();
                    if n < 2 {
                        return;
                    }
                    (s[n - 1], s[n - 2])
                };
                let (Some(offset), Some(size)) = (to_usize(offset), to_usize(size)) else {
                    return;
                };
                if size == 0 || size > MAX_PREIMAGE_LEN {
                    return;
                }
                let preimage = read_mem(interp, offset, size);
                self.preimages.insert(keccak256(&preimage), preimage);
            }
            OP_SLOAD => {
                // Record every SLOAD; `accesses` later filters to hashed slots,
                // while `slots_returning` uses the full set for value-matching.
                if let Some(k) = interp.stack.data().last() {
                    self.pending = Some(word_from(*k));
                }
            }
            _ => {}
        }
    }

    fn step_end(&mut self, interp: &mut Interpreter<INTR>, _ctx: &mut CTX) {
        if let Some(slot) = self.pending.take() {
            // The SLOAD has executed; its result is now on top of the stack.
            if let Some(value) = interp.stack.data().last().copied() {
                self.reads.push(SloadRecord { slot, value });
            }
        }
    }
}

// ===========================================================================
// Resolver internals
// ===========================================================================

/// Factor a hashed `SLOAD` slot into its key chain, base slot, and layout.
fn resolve(
    slot: B256,
    value: U256,
    pre: &HashMap<B256, Vec<u8>>,
    known: &[B256],
) -> HashSlotAccess {
    let mut keys: Vec<B256> = Vec::new();
    let mut confidence = Confidence::High;
    let mut cur = slot;

    let (base_slot, layout) = loop {
        let Some(p) = pre.get(&cur) else {
            // Not a recorded preimage: `cur` is a literal base slot (or a hash
            // we didn't observe).
            let l = if keys.is_empty() {
                SlotLayout::Opaque
            } else {
                SlotLayout::Nested
            };
            break (U256::from_be_slice(cur.as_slice()), l);
        };

        match p.len() {
            64 => {
                let aw = B256::from_slice(&p[0..32]);
                let bw = B256::from_slice(&p[32..64]);
                let a_parent = pre.contains_key(&aw);
                let b_parent = pre.contains_key(&bw);
                if a_parent ^ b_parent {
                    // The known-hash half is the parent location; recurse.
                    let (parent, key) = if a_parent { (aw, bw) } else { (bw, aw) };
                    keys.push(key);
                    cur = parent;
                    continue;
                }
                let (slot_word, key_word, key_first, conf) =
                    split_key_slot(&p[0..32], &p[32..64], known);
                confidence = confidence.min(conf);
                keys.push(key_word);
                let layout = if keys.len() > 1 {
                    SlotLayout::Nested
                } else if key_first {
                    SlotLayout::SolidityMapping
                } else {
                    SlotLayout::VyperMapping
                };
                break (U256::from_be_slice(slot_word.as_slice()), layout);
            }
            32 => {
                // Solady-style packed: address in high 20 bytes + a low seed.
                let hi = Address::from_slice(&p[0..20]);
                if hi != Address::ZERO && known.iter().any(|k| Address::from_word(*k) == hi) {
                    keys.push(hi.into_word());
                    confidence = confidence.min(Confidence::Medium);
                    let seed = U256::from_be_slice(&p[20..32]);
                    break (seed, SlotLayout::PackedSeed { seed });
                }
                // Otherwise a keccak(slot) array/base pointer.
                confidence = confidence.min(Confidence::Heuristic);
                let l = if keys.is_empty() {
                    SlotLayout::ArrayPointer
                } else {
                    SlotLayout::Nested
                };
                break (U256::from_be_slice(&p[0..32]), l);
            }
            _ => {
                confidence = Confidence::Low;
                break (U256::from_be_slice(cur.as_slice()), SlotLayout::Opaque);
            }
        }
    };

    let depth = keys.len().max(1);
    HashSlotAccess {
        slot,
        value,
        keys,
        base_slot,
        layout,
        depth,
        confidence,
    }
}

/// Decide which half of a base-level 64-byte preimage is the key vs the slot.
/// Returns `(slot_word, key_word, key_is_first, confidence)`.
fn split_key_slot(a: &[u8], b: &[u8], known: &[B256]) -> (B256, B256, bool, Confidence) {
    let aw = B256::from_slice(a);
    let bw = B256::from_slice(b);
    let a_known = known.iter().any(|k| word_matches(*k, aw));
    let b_known = known.iter().any(|k| word_matches(*k, bw));
    match (a_known, b_known) {
        (true, false) => (bw, aw, true, Confidence::High), // a is key (first)
        (false, true) => (aw, bw, false, Confidence::High), // b is key (second)
        _ => {
            // No unambiguous known-key anchor: the base slot is the numerically
            // smaller word (a small slot index vs a 20-byte address/key).
            let (sa, sb) = (sig(a), sig(b));
            if sa < sb {
                (aw, bw, false, Confidence::Medium)
            } else if sb < sa {
                (bw, aw, true, Confidence::Medium)
            } else {
                (aw, bw, false, Confidence::Low)
            }
        }
    }
}

/// Read `len` bytes of local EVM memory at `offset`, zero-padding past the
/// current memory size (EVM read-as-zero semantics) so we never panic.
fn read_mem<INTR: InterpreterTypes>(
    interp: &Interpreter<INTR>,
    offset: usize,
    len: usize,
) -> Vec<u8> {
    let mut out = vec![0u8; len];
    let size = interp.memory.size();
    if offset >= size || len == 0 {
        return out;
    }
    let avail = (size - offset).min(len);
    let chunk = interp.memory.slice_len(offset, avail);
    out[..avail].copy_from_slice(&chunk[..]);
    out
}

fn to_usize(x: U256) -> Option<usize> {
    let limbs = x.as_limbs();
    if limbs[1] | limbs[2] | limbs[3] != 0 {
        return None;
    }
    usize::try_from(limbs[0]).ok()
}

fn word_from(x: U256) -> B256 {
    B256::from(x.to_be_bytes::<32>())
}

/// Two words match if equal, or if they denote the same address in their low 20
/// bytes (mapping keys are addresses stored left-padded).
fn word_matches(a: B256, b: B256) -> bool {
    a == b || Address::from_word(a) == Address::from_word(b)
}

/// Significant byte count = 32 minus leading zero bytes.
fn sig(bytes: &[u8]) -> usize {
    match bytes.iter().position(|&x| x != 0) {
        Some(p) => bytes.len() - p,
        None => 0,
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::address;

    fn sol_slot(key: Address, base: u64) -> B256 {
        let mut pre = [0u8; 64];
        pre[0..32].copy_from_slice(key.into_word().as_slice());
        pre[63] = base as u8;
        keccak256(pre)
    }

    fn vyper_slot(base: u64, key: Address) -> B256 {
        let mut pre = [0u8; 64];
        pre[31] = base as u8;
        pre[32..64].copy_from_slice(key.into_word().as_slice());
        keccak256(pre)
    }

    fn solady_slot(key: Address, seed: u32) -> B256 {
        let mut pre = [0u8; 32];
        pre[0..20].copy_from_slice(&key.into_array());
        pre[28..32].copy_from_slice(&seed.to_be_bytes());
        keccak256(pre)
    }

    /// Build a probe with preimages seeded as if the given preimages were hashed.
    fn probe_with(preimages: Vec<Vec<u8>>, reads: Vec<(B256, U256)>) -> HashStorageProbe {
        let mut p = HashStorageProbe::new();
        for pre in preimages {
            p.preimages.insert(keccak256(&pre), pre);
        }
        for (slot, value) in reads {
            p.reads.push(SloadRecord { slot, value });
        }
        p
    }

    #[test]
    fn slots_returning_matches_value_and_dedups() {
        // Two hashed reads + one plain read; slot_plain and one mapping slot
        // share a value.
        let key = address!("00000000000000000000000000000000000000A1");
        let mut pre = vec![0u8; 64];
        pre[0..32].copy_from_slice(key.into_word().as_slice());
        pre[63] = 3;
        let hashed_slot = keccak256(&pre);
        let plain_slot = B256::from(U256::from(2u64).to_be_bytes::<32>()); // e.g. totalSupply

        let mut probe = probe_with(vec![pre], vec![]);
        // hashed slot read twice with value 100; plain slot read once with 100.
        probe.reads.push(SloadRecord {
            slot: hashed_slot,
            value: U256::from(100u64),
        });
        probe.reads.push(SloadRecord {
            slot: hashed_slot,
            value: U256::from(100u64),
        });
        probe.reads.push(SloadRecord {
            slot: plain_slot,
            value: U256::from(100u64),
        });
        probe.reads.push(SloadRecord {
            slot: plain_slot,
            value: U256::from(7u64),
        }); // different value

        // Value 100 → the hashed slot then the plain slot (deduped, in order).
        assert_eq!(
            probe.slots_returning(U256::from(100u64)),
            vec![hashed_slot, plain_slot]
        );
        // Plain slots participate even though they are not in `preimages`.
        assert_eq!(probe.hashed_read_count(), 2); // both hashed reads counted; plain excluded
        assert_eq!(probe.accesses(&[key.into_word()]).len(), 2); // only hashed reads resolve
    }

    #[test]
    fn resolves_solidity_mapping() {
        let key = address!("00000000000000000000000000000000000000A1");
        let mut pre = vec![0u8; 64];
        pre[0..32].copy_from_slice(key.into_word().as_slice());
        pre[63] = 3;
        let slot = keccak256(&pre);
        let probe = probe_with(vec![pre], vec![(slot, U256::from(42u64))]);

        let a = &probe.accesses(&[key.into_word()])[0];
        assert_eq!(a.layout, SlotLayout::SolidityMapping);
        assert_eq!(a.base_slot, U256::from(3u64));
        assert_eq!(a.keys, vec![key.into_word()]);
        assert_eq!(a.confidence, Confidence::High);
    }

    #[test]
    fn resolves_vyper_order_without_known_key() {
        // A realistic (full-width) address so the magnitude heuristic can
        // separate the 20-byte key from the small slot index.
        let key = address!("28C6c06298d514Db089934071355E5743bf21d60");
        let mut pre = vec![0u8; 64];
        pre[31] = 2;
        pre[32..64].copy_from_slice(key.into_word().as_slice());
        let slot = keccak256(&pre);
        let probe = probe_with(vec![pre], vec![(slot, U256::from(1u64))]);

        // No known keys: the magnitude heuristic must still find slot-first.
        let a = &probe.accesses(&[])[0];
        assert_eq!(a.layout, SlotLayout::VyperMapping);
        assert_eq!(a.base_slot, U256::from(2u64));
        assert_eq!(a.confidence, Confidence::Medium);
    }

    #[test]
    fn resolves_solady_packed() {
        let key = address!("00000000000000000000000000000000000000A1");
        let seed = 0x87a2_11a2u32;
        let mut pre = vec![0u8; 32];
        pre[0..20].copy_from_slice(&key.into_array());
        pre[28..32].copy_from_slice(&seed.to_be_bytes());
        let slot = keccak256(&pre);
        let probe = probe_with(vec![pre], vec![(slot, U256::from(7u64))]);

        let a = &probe.accesses(&[key.into_word()])[0];
        assert_eq!(
            a.layout,
            SlotLayout::PackedSeed {
                seed: U256::from(seed)
            }
        );
        assert!(a.keyed_by(key.into_word()));
    }

    #[test]
    fn resolves_nested_mapping() {
        let owner = address!("00000000000000000000000000000000000000A1");
        let spender = address!("00000000000000000000000000000000000000B2");
        // inner = keccak(owner ‖ 4); outer = keccak(spender ‖ inner)
        let mut inner_pre = vec![0u8; 64];
        inner_pre[0..32].copy_from_slice(owner.into_word().as_slice());
        inner_pre[63] = 4;
        let inner = keccak256(&inner_pre);
        let mut outer_pre = vec![0u8; 64];
        outer_pre[0..32].copy_from_slice(spender.into_word().as_slice());
        outer_pre[32..64].copy_from_slice(inner.as_slice());
        let outer = keccak256(&outer_pre);

        let probe = probe_with(vec![inner_pre, outer_pre], vec![(outer, U256::from(9u64))]);
        let a = &probe.accesses(&[owner.into_word(), spender.into_word()])[0];
        assert_eq!(a.layout, SlotLayout::Nested);
        assert_eq!(a.base_slot, U256::from(4u64));
        assert_eq!(a.depth, 2);
        assert_eq!(a.keys, vec![spender.into_word(), owner.into_word()]);
    }

    #[test]
    fn tracked_mapping_round_trips_each_layout() {
        let key = address!("00000000000000000000000000000000000000A1");

        let t = TrackedMapping::new(Address::ZERO, U256::from(3u64), SlotLayout::SolidityMapping);
        assert_eq!(t.slot_for(key.into_word()).unwrap(), sol_slot(key, 3));

        let t = TrackedMapping::new(Address::ZERO, U256::from(2u64), SlotLayout::VyperMapping);
        assert_eq!(t.slot_for(key.into_word()).unwrap(), vyper_slot(2, key));

        let seed = 0x87a2_11a2u32;
        let t = TrackedMapping::new(
            Address::ZERO,
            U256::from(seed),
            SlotLayout::PackedSeed {
                seed: U256::from(seed),
            },
        );
        assert_eq!(t.slot_for(key.into_word()).unwrap(), solady_slot(key, seed));
    }

    #[test]
    fn as_tracked_rejects_nested_and_arrays() {
        let access = HashSlotAccess {
            slot: B256::ZERO,
            value: U256::ZERO,
            keys: vec![B256::ZERO, B256::ZERO],
            base_slot: U256::from(4u64),
            layout: SlotLayout::Nested,
            depth: 2,
            confidence: Confidence::High,
        };
        assert!(access.as_tracked(Address::ZERO).is_none());
    }
}
