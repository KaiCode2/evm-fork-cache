# Internals — the copy-on-write snapshot cost model

> This is an implementation note, not a value-prop benchmark. For the numbers a
> consumer cares about (fetch minimization, candidate throughput, reactive sync,
> optimistic latency) see the **Performance** section of the [README](../README.md).

`create_snapshot()` is the operation a search loop runs once per block before
fanning candidates out. Phase 5 replaced its original O(total state) deep clone
with a two-tier copy-on-write view: the cold `BlockchainDb` index (layer 2) is
flattened once into an immutable, `Arc`-shared base (per-account storage shared by
`Arc`), memoized across snapshots and rebuilt copy-on-write only for changed
addresses; each snapshot then folds just the hot CacheDB delta (layer 1). Reads
stay O(1) and bit-for-bit identical to the deep clone (pinned by the differential
gate in `tests/cow_snapshot.rs`).

The retained `create_snapshot_deep_clone()` is kept as (a) the read-equivalence
reference and (b) the A/B regression baseline, so a future change can't silently
regress `create_snapshot` back toward O(total state). The `create_snapshot` group
in `benches/simulation.rs` measures both.

Indicative A/B (Apple M1 Pro, `aarch64-apple-darwin`, Criterion medians; offline,
state injected directly — read the ratio, not the absolute):

| Cache size (accounts × slots) | Deep clone | COW `create_snapshot` | Ratio |
|:-----------------------------:|:----------:|:---------------------:|:-----:|
| 100 × 8     | 53 µs   | 2.1 µs | ~25× |
| 1,000 × 8   | 791 µs  | 19 µs  | ~41× |
| 2,000 × 16  | 3.2 ms  | 52 µs  | ~61× |
| 5,000 × 16  | 9.5 ms  | 113 µs | ~84× |
| 10,000 × 16 | 16.5 ms | 214 µs | ~77× |

The deep clone copies every account and slot on every snapshot (O(total state));
the COW snapshot shares the memoized base and folds only the hot delta, so its
cost tracks *changed* state, not *total* state. This is what makes the
[per-block fan-out](../README.md#performance) cheap — but it is an internal cost
model, which is why it lives here rather than in the README headline.

**Residual cost (honest):** a COW snapshot is not free. When layer 2 is unchanged
it still pays an O(accounts) length-scan of the layer-2 maps plus an O(layer-1)
fold of the hot delta; a full rebuild (first snapshot, or after `set_block`) is
still O(total state). The decisions and cost model are summarized in
[`ROADMAP.md`](ROADMAP.md), and the residual is recorded in
[`KNOWN_ISSUES.md`](KNOWN_ISSUES.md).

Reproduce: `cargo bench --bench simulation` (the `create_snapshot` and
`resnapshot_hot_loop` groups).
