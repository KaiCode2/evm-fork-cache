# Verified code seeding & local etch (spec)

Status: **implemented in 0.2.0** — C1–C5 landed (`3c2e46a`, `9c0e80d`,
`396ecad`, `976dd56`, `bab4caa`); all locked decisions below are as-built.
Consumers: `evm-amm-state` ships its initial release on the canonical path.

Also carries a companion workstream (§6): batched `eth_getProof` fan-out and
a root-gate cadence. Review call: `eth_getProof` is the slowest read the
crate issues, and anything issued per block gets expensive quick — per-block
proofing is retired as a default.

The feature adds a first-class answer to "how does code get into the cache
without an `eth_getCode`?" — with two deliberately distinct trust classes:

1. **Canonical seed** — the adapter pushes bytecode it *claims* is the
   on-chain code (a patched Uniswap V3 pool template, a shared V2 pair blob).
   The cache verifies the claim once against the on-chain code hash, then
   marks it verified forever. Mismatch ⇒ purge + resync from RPC.
2. **Local etch** — the adapter pushes bytecode that is *deliberately not*
   on-chain (an unreleased searcher contract, a test harness). Never
   verified, never mistaken for chain state, visible as divergence.

## Motivation

- Cold start today materializes each account lazily via `SharedBackend::
  basic_ref` → `eth_getBalance` + `eth_getTransactionCount` + `eth_getCode`
  per address. For N pools that is 3N round trips and megabytes of hex code
  payload; the code bytes are data the adapter already possesses.
- `bytecodes.bin` removes the cost on *warm* restarts only. Fresh machines,
  wiped caches, and — critically — **newly deployed pools detected while the
  cache is live** all pay the full fetch.
- The verification read is nearly free: `EXTCODEHASH` for thousands of
  addresses fits in **one** `eth_call` via the already-shipped
  `ACCOUNT_FIELDS_EXTRACTOR_CODE` (~5.3k gas/address), and `eth_getProof`
  responses already carry `codeHash` through `AccountProof` unused.
- The existing divergence primitives (`override_account_code*`,
  `deploy_contract`) copy code between accounts or run creation code; there
  is no raw-bytes write, and nothing records *which* accounts diverge from
  chain — the health surface cannot report it.

Target outcome for the live-registration path: a newly detected V3 pool goes
from "factory event seen" to "fully materialized, code-verified, storage
synced" in **~1 round trip and zero `eth_getCode`**.

## 0. Ground rules

- **Fail-closed on trust, fail-safe on transport.** A *successful* hash
  comparison that mismatches purges the seed. A *failed* verification call
  (transport error, missing fetcher) leaves the seed `Pending` and reports it
  — it never silently promotes to `Verified` and never destroys the seed.
- **`Pending` never masquerades as chain-fetched.** Marks persist across
  save/load, including `Pending`. An unverified seed that survives a restart
  is still unverified.
- **Discover/sims never run over unverified canonical seeds** in the
  cold-start driver: code verification is the *first* phase of a round.
- **Etched state is always distinguishable.** Every non-RPC, non-canonical
  code write records an `Etched` mark; the set is queryable in one place.
- All new behavior is offline-testable with stub fetchers, per house rules.

## 1. Objective & scope

In scope:
- Per-address code-seed marks (`Pending` / `Verified` / `Etched`) with
  persistence (`code_seeds.bin`) and conflict rules.
- `seed_account_code` / `etch_account_code` write primitives.
- An `AccountFieldsFetchFn` seam (sync, type-erased, default-wired to
  `fetch_account_fields_bulk`) + `verify_code_seeds()`.
- A `verify_code` cold-start driver phase (runs before `accounts`).
- Snapshot-generation and health-surface semantics for both classes.
- Companion (§6): single-invocation proof batching at both reactive call
  sites, a bounded-concurrency default proof fetcher, and `RootGateCadence`
  (per-block proofing stops being the default).

Out of scope (explicit non-goals):
- Content-addressing / deduplicating `bytecodes.bin` (N identical V2 blobs
  are stored N times today; orthogonal disk optimization).
- Verifying nonce/balance beyond what the fields sample provides (exact
  nonce needs the proof path; only matters for contracts that `CREATE`).
- EIP-7702 delegated EOAs (`EXTCODEHASH` returns the hash of the delegation
  designator; not an AMM shape — documented, not handled).
- Pre-Cancun `SELFDESTRUCT`-then-redeploy code mutation. Post-EIP-6780,
  deployed code is immutable, so `Verified` is durable. On chains without
  6780 the escape hatch is `purge_account` (clears the mark).

## 2. What already exists — build on, do not rebuild

| Piece | Where | Role here |
|---|---|---|
| `ACCOUNT_FIELDS_EXTRACTOR_CODE` + `fetch_account_fields_bulk` | `src/bulk_storage.rs:1035,1057` | one `eth_call` returns `(balance, EXTCODEHASH)` for every seeded address |
| `AccountFieldsSample` | `src/bulk_storage.rs:1041` | `code_hash` zero ⇒ nonexistent; `keccak256("")` ⇒ codeless (EOA) |
| `AccountProof.code_hash` | `src/cache/mod.rs:110` | free future piggyback wherever `probe_roots` already proofs an address |
| `purge_account` | `src/cache/mod.rs:3138` | the mismatch resync primitive (both layers; bumps generation via `apply_update`) |
| `ensure_account` early-return | `src/cache/mod.rs:3953` | a seeded (present) account never triggers the `basic_ref` RPC triple |
| Cold-start phase order | `src/cold_start/driver.rs:87` | `accounts → verify → probe → probe_roots → discover`; `verify_code` slots in front |
| Versioned envelope persistence | `src/cache/versioned.rs`, `roots.bin`/`bytecodes.bin` pattern | `code_seeds.bin` reuses it; load at `mod.rs:1643`, save at `mod.rs:1988`, path from `CacheConfig` (`metadata.rs`) |
| Sync fetcher bridging | `point_read_storage_fetcher`, `block_in_place_handle` | `AccountFieldsFetchFn` default wiring is the same shape (multi-thread runtime requirement inherited) |
| `run_root_gate` / account-resync proof loops | `src/reactive/mod.rs:1993,2938` | per-address seam calls today — §6.1 collapses each into one batched invocation |
| Default account-proof fetcher | `src/cache/mod.rs:1717` | serial await loop — §6.1 makes it a bounded-concurrency fan-out |
| Cold-start `probe_roots` batching | `src/cold_start/driver.rs:176` | already one seam call for the whole list — the call shape §6.1 mirrors |

## 3. Core types & behavior

### 3.1 Marks (`CodeSeedState`)

```rust
/// Provenance + trust state of an address's cached bytecode, for code that
/// did NOT arrive via the lazy RPC backend. Absence of a mark means
/// RPC-origin (fetched from the provider, trusted as chain state).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CodeSeedState {
    /// Canonical claim awaiting on-chain code-hash verification.
    Pending { code_hash: B256 },
    /// Canonical claim confirmed against the chain. Never re-verified.
    Verified { code_hash: B256, verified_at_block: u64 },
    /// Deliberate local divergence. Never verified, excluded from all
    /// canonical machinery, reported on the health surface.
    Etched { code_hash: B256 },
}
```

Stored as `HashMap<Address, CodeSeedState>` on `EvmCache`. Getters:
`code_seed_state(&addr)`, `pending_code_seeds() -> Vec<Address>`,
`etched_accounts() -> Vec<Address>` (health surface).

### 3.2 Write primitives

```rust
/// Seed canonical runtime code for `address` without fetching it.
/// Marks the seed `Pending` until `verify_code_seeds` confirms it.
/// Defaults: nonce 1 (EIP-161 contract minimum; exact only for contracts
/// that never `CREATE`), balance ZERO until verification patches the real
/// value from the same response. Returns the recorded keccak code hash.
pub fn seed_account_code(&mut self, address: Address, code: Bytes) -> Result<B256>;
pub fn seed_account_code_with(
    &mut self, address: Address, code: Bytes, nonce: u64, balance: U256,
) -> Result<B256>;

/// Etch deliberately-local runtime code at `address` (raw-bytes sibling of
/// `override_or_create_account_code`, no source account needed). Marks
/// `Etched`; preserves existing balance/nonce/storage when present.
pub fn etch_account_code(&mut self, address: Address, code: Bytes) -> Result<B256>;
```

Write mechanics (both): build `Bytecode::new_raw` + `hash_slow`, insert into
the CacheDB overlay **and** BlockchainDb accounts (the
`override_account_code_with_missing_target` dual-layer pattern,
`mod.rs:5135`), `mark_base_dirty`. Because the account is then present in
both layers, `basic_ref` never fires for it — that alone deletes the RPC
triple for seeded accounts.

**Conflict rules for `seed_account_code`** (chain-fetched state is
authoritative over templates):

| Existing state at `address` | Seed behavior |
|---|---|
| Absent | insert, mark `Pending` |
| Present, RPC-origin, same code hash | keep, mark `Verified` immediately (warm caches verify for free, zero RPC) |
| Present, RPC-origin, different hash (incl. codeless EOA) | `Err(CacheError::CodeSeedConflict { address, cached, seeded })` — never clobber known chain code with a template; adapter purges first if it believes the chain moved |
| Present, `Pending`/`Verified`/`Etched` | overwrite, mark `Pending` (re-seed restarts the claim) |

`etch_account_code` never conflicts: it overwrites anything and marks
`Etched` (divergence is the caller's explicit intent).

**Unification (locked):** `override_account_code_with_missing_target` and
`deploy_contract` also record `Etched` for their target/created address, so
*every* locally-divergent code site is visible through `etched_accounts()`.
No other behavior of those APIs changes.

### 3.3 Verification (`AccountFieldsFetchFn` seam + `verify_code_seeds`)

```rust
/// Callback fetching `(balance, EXTCODEHASH)` samples for many addresses at
/// a pinned block — one bulk `eth_call` by default. Sync, type-erased, same
/// bridging rules as `StorageBatchFetchFn`.
pub type AccountFieldsFetchFn = Arc<
    dyn Fn(Vec<Address>, BlockId) -> StorageFetchResult<Vec<(Address, AccountFieldsSample)>>
        + Send + Sync,
>;

pub fn set_account_fields_fetcher(&mut self, f: AccountFieldsFetchFn);
pub fn account_fields_fetcher(&self) -> Option<&AccountFieldsFetchFn>;

/// Verify every `Pending` seed against the chain at the pinned block, in one
/// fields call. Match ⇒ `Verified` + real balance injected. Mismatch ⇒
/// `purge_account` + reported (re-fetch is the caller's/driver's next step).
/// Transport failure ⇒ everything stays `Pending`, reported unverifiable.
pub fn verify_code_seeds(&mut self) -> Result<CodeVerifyReport>;

pub struct CodeVerifyReport {
    pub verified: Vec<Address>,
    pub mismatched: Vec<CodeMismatch>,   // { address, expected: B256, actual: B256 } — purged
    pub not_deployed: Vec<Address>,      // EXTCODEHASH == 0 at pinned block — purged
    pub codeless: Vec<Address>,          // EXTCODEHASH == keccak("") (EOA) — purged
    pub unverifiable: Vec<(Address, String)>, // fetch failed — still Pending
}
```

Per-outcome semantics:
- **Match** ⇒ mark `Verified { verified_at_block }`; patch the account's
  balance from the sample (materialization of pinned-block truth — see §3.6;
  nonce keeps the seeded value).
- **Mismatch / not-deployed / codeless** ⇒ `purge_account` (clears both
  layers *and* the mark; generation bumps via the purge path, so downstream
  fan-out sees the change). The next touch — driver `accounts` phase or lazy
  `basic_ref` — refetches real chain state.
- **`not_deployed`** deserves its own bucket: it is the live-registration
  race (factory event from block N+k, cache pinned at N). The adapter's
  correct response is retry-after-re-pin, not template debugging.

Constructor wiring: the provider-backed constructor installs the default
fetcher (bridging `fetch_account_fields_bulk`); `from_backend` leaves it
`None` (symmetric with the other three seams at `mod.rs:1957`). One fields
call covers ~10k addresses inside an 80M-gas envelope; chunking is deferred
until a consumer approaches that (documented ceiling).

Host-address caveat (documented, inherited from the extractor): a seeded
address equal to `MULTICALL3_ADDRESS` cannot be verified by the fields path
(the extractor is hosted there under the override) — reported
`unverifiable`; use the proof path for it.

### 3.4 Persistence (`code_seeds.bin`)

- Versioned envelope (`EFCSEED\0`, v1), bincode `HashMap<Address,
  CodeSeedState>`; `Option`-tolerant load like `roots.bin` (missing/legacy
  file ⇒ empty map, never an error — old caches upgrade cleanly).
- Path from `CacheConfig` beside `bytecodes.bin` (`metadata.rs`).
- Load in the constructor next to the bytecode seeding (`mod.rs:1643`) —
  marks must be restored *whenever* `bytecodes.bin` code is restored.
- Save in the persistence path (`mod.rs:1988`) **before** `bytecodes.bin`:
  if the process dies between the two writes, marks-without-code is harmless
  (a mark for absent code is ignored/pruned on load); code-without-marks
  would let a `Pending` seed masquerade as RPC-origin. Fail-closed ordering.
- `Pending` persists as `Pending`; `Etched` persists as `Etched` (a reloaded
  searcher contract is still divergence); `Verified` persists and is never
  re-verified.

### 3.5 Cold-start driver: `verify_code` phase

New phase order: **verify_code → accounts → verify → probe → probe_roots →
discover** (`driver.rs:87`).

- Implicit work set: `pending_code_seeds()` — no new `ColdStartPlan` field.
  Seeds are cache state the adapter just wrote; duplicating them in the plan
  invites drift.
- Guard (mirrors `NoBatchFetcher`/`NoAccountProofFetcher`): a round with
  pending seeds and no fields fetcher short-circuits with
  `ColdStartError::NoAccountFieldsFetcher` before issuing any read.
- Outcome recorded as `ColdStartResults.code_verifications:
  Option<CodeVerifyReport>`; it runs first, so an `accounts`-phase hard error
  preserves it (never `NotAttempted`).
- Mismatched/purged addresses that also appear in `plan.accounts` are
  refetched by the very next phase — resync falls out of existing machinery.
- `unverifiable` (transport) is **not** a round hard error (matches
  probe_roots' per-address-failure stance), but it is visible in the report;
  adapters that require verified code before serving gate on the report.
- Ordering rationale: `discover` executes sims; they must never run over an
  unverified canonical claim.
- Idempotent across rounds: the `Pending` set empties after the first
  successful pass.

### 3.6 Snapshot-generation & verdict semantics (WS-8/WS-9 coherence)

- `seed_account_code` and `etch_account_code` **bump** the snapshot
  generation: they change executable state. (The no-bump exemption stays
  storage-prefetch-only: `inject_storage_batch` materializes values the
  pinned chain already had; a seed is a *claim*, an etch is a *mutation*.)
- `verify_code_seeds` on match does **not** bump: confirming a claim and
  patching balance to pinned-block truth is materialization. On mismatch the
  purge bumps (existing `apply_update` path) — exactly when readers must
  notice.
- Freshness verdicts are unchanged by design: the validator's verify set is
  storage slots; code is never in it. Etched code re-executes identically in
  validator overlays (same cache), so `Confirmed*` verdicts over etched
  contracts remain honest — they attest storage freshness, not code
  canonicality, and the etched set is queryable for consumers who care.
- Root-gate hygiene (documented, not enforced): adapters should not list
  etched accounts in `probe_roots` if they also locally mutate that
  account's storage — the storageHash gate would report expected noise.

## 4. Live registration — the new-pool path (primary consumer flow)

Cache already running; factory event announces pool `P` with known
`(token0, token1, fee, tickSpacing)`:

1. Adapter patches its runtime template → `seed_account_code(P, code)`
   (no RPC; generation bumps).
2. Fire **two independent `eth_call`s concurrently** (both pinned to the
   same block):
   a. `verify_code_seeds()` — settles the claim, injects real balance.
   b. the V3 full-sync `StorageProgram` — injects the pool's entire tick
      range/liquidity/observations.
   They must be separate calls: the sync program *overrides P's code* inside
   its call, so `EXTCODEHASH(P)` in a merged multicall would hash the
   override, not the deployment. Concurrent issue keeps it ~1 RTT.
3. Storage injections are valid regardless of the verify outcome (chain
   storage is code-independent). On mismatch: `P` was purged — re-seed with
   a corrected template or let `ensure_account` fetch real code, then
   re-inject the (still valid) storage snapshot. On `not_deployed`: re-pin
   forward, retry.

Net: zero `eth_getCode`, zero `basic_ref` triple, one round trip in the
happy path. Cold start is the same story ×N pools: seed all, one fields
call verifies the fleet, `ensure_account` triples only run for failures.

## 5. Consumer contract (evm-amm-state, informative)

- **V2-style pairs / EIP-1167 clones:** one blob (or one designator) per
  factory per chain covers every instance — immutable-free runtime code is
  byte-identical, so one stored template seeds unbounded pools.
- **V3-style pools:** `factory/token0/token1/fee/tickSpacing/
  maxLiquidityPerTick` are Solidity `immutable`s **baked into runtime
  code** — every pool's code hash differs. The adapter stores one template
  per (factory, chain, compiler build) plus the immutable byte offsets
  (discoverable by diffing two known pools' deployed code), patches per
  pool, and lets code-hash verification catch any wrong offset/compiler
  variant — mismatch degrades to one `eth_getCode`, never to wrong sims.
- Seed-state file and marks live entirely in evm-fork-cache; the adapter's
  only obligations are (a) seed before cold start / on detection, (b) gate
  serving on `CodeVerifyReport` if it requires verified code, (c) keep
  etched addresses out of `probe_roots` it also mutates.

## 6. Companion workstream: proof batching & root-gate cadence

Same release window, separate commits. The reactive root gate today issues
one **serial** `eth_getProof` per tracked account per canonical block — the
default fetcher's own comment encodes the assumption being retired here
("Account targets are few, so a straightforward per-request loop … is
sufficient", `mod.rs:1715`). Two changes fix the shape:

### 6.1 Batch the proof fan-out (call shape + default fetcher)

- **Call sites**: `run_root_gate` (`reactive/mod.rs:1993`) and the
  account-resync executor (`reactive/mod.rs:2938`) each invoke the seam once
  per address. Both switch to **one seam invocation carrying the full target
  list**, exactly as the cold-start `probe_roots` phase already does
  (`driver.rs:176`). Failure handling is untouched: the contract already
  returns per-address `Result`s, and both callers already treat a
  failed/omitted entry as "no signal this round".
- **Default fetcher** (`mod.rs:1717`): replace the serial await loop with a
  bounded, order-preserving fan-out
  (`futures::stream::iter(..).buffered(cap)`). `eth_getProof` is
  single-address at the RPC level, so concurrency is the only lever:
  wall-clock per firing drops from `N × RTT` to `~ceil(N / cap) × RTT`.
- **Knob**: `EvmCacheBuilder::max_concurrent_proofs(usize)`, default **8**
  (name-symmetric with `BulkCallConfig::max_concurrent_calls`). The seam
  signature is unchanged; custom fetchers keep working as-is.

### 6.2 `RootGateCadence` — per-block proofing is never the default

```rust
/// How often the reactive root gate probes tracked accounts.
pub enum RootGateCadence {
    /// Probe at most once every `n` canonical blocks. `EveryNBlocks(1)` is
    /// the old per-block behavior; the default is 16.
    EveryNBlocks(NonZeroU64),
    /// Root gate off: coverage gaps surface only via decoders + freshness.
    Disabled,
}
```

- Field + setter on `ReactiveRuntime`, default `EveryNBlocks(16)`. Applies
  to `WholeAccount` and `Scalars` alike (`Slots` was never gated).
- **Why skipping blocks is safe**: the gate diffs `root_now` against the
  *persisted baseline*, never block-over-block (phase-8 §6: a currency gate
  across time). A move in any skipped block is still visible at the next
  probe — cadence trades detection lag (≤ `n−1` blocks) for cost, never
  eventual detection. Missed-block ranges stay caught for the same reason.
- **Correctness obligation — accumulate `touched`**: the gap rule is "root
  moved ∧ addr ∉ touched". Under cadence, `touched` must be the **union of
  decoder-touched addresses since the last firing**
  (`touched_since_gate: HashSet<Address>` on the runtime, drained when the
  gate fires). Without accumulation, a decoder-covered write in a skipped
  block would false-positive as a `CoverageGap` at the next probe.
- Firing rule: the first canonical block ever seen fires the gate (baseline
  adoption must not wait `n` blocks); thereafter at most once per `n`.
  Accounts registered mid-flight adopt at the next scheduled firing (≤ `n`
  blocks; pre-adoption writes are not gap-checked — same as today, just up
  to `n` blocks later).
- Cost shape, 100 `WholeAccount` pools on mainnet: per-block serial ≈ 600k
  CU/h of `eth_getProof`; every-16-blocks ≈ 37k CU/h, with wall-clock per
  firing cut ~8× by §6.1. Fast-block L2s should raise `n`, not lower it.

## 7. Tests — the acceptance contract (offline, stub fetchers)

1. Seed → `Pending`; stubbed match ⇒ `Verified`, balance patched, fields
   fetcher **called exactly once ever** (call-count proves no
   re-verification, including across a save/load).
2. Stubbed mismatch ⇒ purged from both layers, mark cleared, reported;
   subsequent `ensure_account` refetches via stub backend.
3. `not_deployed` (zero hash) and `codeless` (`keccak("")`) classified into
   their buckets, purged.
4. Transport failure ⇒ still `Pending`, reported `unverifiable`, nothing
   purged.
5. Seed over RPC-origin equal-hash ⇒ instant `Verified`, no fetcher call;
   over RPC-origin different-hash ⇒ `CodeSeedConflict`, cached code intact.
6. Etch ⇒ `Etched`, excluded from `verify_code_seeds`, sims read etched
   code, `etched_accounts()` reports it; `override_account_code*` and
   `deploy_contract` targets also appear.
7. Persistence round-trip: `Pending`/`Verified`/`Etched` survive save/load;
   unmarked (RPC-origin) accounts untouched; missing `code_seeds.bin` loads
   as empty (legacy caches); mark-without-code pruned on load.
8. Generation: seed bumps, etch bumps, verify-match does not, mismatch-purge
   does (extends `snapshot_generation_bumps_on_writes_and_repins_not_prefetch`).
9. Driver: `verify_code` runs before `accounts` (mismatch in round N is
   refetched by round N's accounts phase); guard fires as
   `NoAccountFieldsFetcher` only for pending-bearing rounds; report survives
   an accounts-phase hard error.
10. Seeded account never triggers `basic_ref` (call-count on stub backend).
11. Root gate fires on the first canonical block, then only on cadence
    boundaries; `Disabled` never invokes the seam; `EveryNBlocks(1)`
    reproduces the old per-block behavior.
12. Touched accumulation: a decoder-covered write in a skipped block does
    **not** report a `CoverageGap` at the next firing (and the accumulator
    drains); an uncovered root move still does.
13. One seam invocation per gate firing and per account-resync batch,
    carrying every target (stub fetcher records call count + batch size).
14. A root move occurring in a skipped block is detected at the next firing
    — cadence delays detection, never loses it.
15. (RPC-gated, `#[ignore]`) fan-out sanity: a ~50-address proof batch
    through the default fetcher completes in ≪ 50 × single-proof latency.

## 8. Decisions (locked)

1. Two classes, three marks; absence of mark = RPC-origin.
2. Chain-fetched code beats templates: equal-hash seeds verify free,
   conflicting seeds error, never overwrite.
3. Verification is one bulk fields call; proof-based (`AccountProof.
   code_hash`) verification is a later zero-RPC optimization, not v1.
4. Mismatch ⇒ purge + report; refetch rides existing paths. Transport
   failure ⇒ keep `Pending`.
5. `Verified` is durable (post-6780 immutability); escape hatch is
   `purge_account`.
6. Persist all three marks; save marks before code (fail-closed ordering).
7. Driver phase is implicit over `pending_code_seeds()`, runs first,
   guarded like the other fetcher-dependent phases.
8. Seed/etch bump the snapshot generation; verify-match does not.
9. `override_account_code*` / `deploy_contract` record `Etched`.
10. Root-gate and account-resync proof reads are single batched seam
    invocations; the default fetcher fans out with bounded, order-preserving
    concurrency (`max_concurrent_proofs`, default 8).
11. Root gate defaults to `EveryNBlocks(16)`; per-block is opt-in, never a
    default. `touched` accumulates across skipped blocks, drained per firing.
12. Cold-start `probe_roots` is untouched (already batched; once per boot).

## 9. Open decisions (confirm before building)

1. **Nonce refinement via proofs** — when an account-proof fetcher is
   installed, `verify_code_seeds` *could* also patch exact nonce/balance
   from `eth_getProof`. Recommend: defer (fields sample suffices for AMMs;
   revisit with a consumer that CREATEs).
2. **`etch_account_code` naming** — `etch` (foundry vocabulary, proposed)
   vs. `set_local_account_code`. Recommend `etch`.
3. **Auto-verify outside the driver** — should the reactive runtime also
   sweep `Pending` seeds on a timer/trigger, or is adapter-driven
   `verify_code_seeds()` + the driver phase enough for v1? Recommend:
   adapter-driven for v1 (the live-registration flow calls it explicitly).
4. **Time-based cadence** (`MinInterval(Duration)`) for chains with
   irregular block times. Recommend: defer — blocks are the ingest loop's
   native unit and need no clock injection in tests.
5. **Per-account cadence overrides** (gate a flagship pool tighter than the
   fleet). Recommend: defer until a consumer asks.

## 10. Build order (commit per step, green each time)

- **C1** — marks + `seed_account_code`/`etch_account_code` + conflict rules
  + `Etched` unification + generation semantics + `code_seeds.bin`
  persistence (tests 1-partial, 5, 6, 7, 8, 10).
- **C2** — `AccountFieldsFetchFn` seam + default wiring +
  `verify_code_seeds` + `CodeVerifyReport` (tests 1–4).
- **C3** — driver `verify_code` phase + `NoAccountFieldsFetcher` +
  `ColdStartResults.code_verifications` (test 9); docs: README feature
  bullet + production-checklist line, INTERNALS section, CHANGELOG 0.2.0
  Added entry, KNOWN_ISSUES note for the pre-6780 caveat.
- **C4** — batched proof fan-out: single seam invocation in `run_root_gate`
  and the account-resync executor; `buffered(cap)` default fetcher +
  `EvmCacheBuilder::max_concurrent_proofs` (tests 13, 15).
- **C5** — `RootGateCadence` + `touched_since_gate` accumulation + firing
  rules (tests 11, 12, 14); CHANGELOG: the root gate's default moves from
  per-block to every-16-blocks inside the existing 0.2.0 entry (branch is
  unpublished, so it is a pre-release default choice, not a breaking
  change).

Estimated at the established quality bar: ~1.5 focused days (C1–C3 ≈ one
day, C4–C5 ≈ a half; no published-surface semantics change anywhere).

## 11. Release integration

Recommendation: **fold into 0.2.0.** The crate is unpublished (no semver
cost), `evm-amm-state`'s initial release wants the behavior, and the two
risks that argued for deferral — unverified-seed persistence and verdict
interaction — are resolved by §3.4 and §3.6 rather than left to
implementation judgment. The docs review then covers the final surface once
instead of twice.

The companion workstream strengthens the case: C5 changes the root gate's
*default* behavior, and defaults are exactly what you want settled before
the first `cargo publish` — the published 0.2.0 should never have shipped a
per-block `eth_getProof` loop as its out-of-the-box posture.
