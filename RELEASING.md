# Releasing

`evm-fork-cache` 0.4.1 is an unpublished additive telemetry candidate based on
published 0.4.0 (`ab08a5702982eba87d8d590120ef1076de8a3342`, confirmed by that
registry archive's VCS metadata). It adds generic optional receipt/dequeue/decode
provenance and preserves existing constructors and execution semantics. No
exporter, bot label, provider operation or runtime task is added. See
`docs/transport-receipt-timing.md` for the Alloy integration and disabled costs.
The candidate also includes the reviewed Rust1.90 checkpoint CAS compatibility
fix and release-lockfile TLS audit patches. It does not alter durable formats.

The registry API confirmed 0.4.1 absent on 2026-10-20. Recheck availability before
an explicitly authorized publication. No tag or package has been published by
this preparation. Complete the unchanged checks below on the candidate and
inspect the packaged source. Wait for the authorized cache publication before
qualifying AMM's exact registry dependency; a local/Git patch is integration
evidence only. No release step is automatic: use clean, reviewed commits and
never publish from a credential-bearing working tree.

## Preflight

```bash
cargo fmt --all -- --check
git diff --check
cargo test --locked --all-targets --all-features
cargo test --locked --doc --all-features
cargo clippy --locked --all-targets --all-features --no-deps -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --locked --all-features --no-deps
cargo check --locked --no-default-features
cargo check --locked --no-default-features --features reactive
cargo check --locked --no-default-features --features reactive-polling
cargo check --locked --no-default-features --features reactive-ws
cargo check --locked --no-default-features --features raw-flashblocks-json
cargo clippy --locked --all-targets --no-default-features --features reactive-polling --no-deps -- -D warnings
cargo test --locked --no-default-features --features reactive-polling
cargo clippy --locked --lib --test raw_json_flashblocks --no-default-features --features raw-flashblocks-json --no-deps -- -D warnings
cargo test --locked --no-default-features --features raw-flashblocks-json --test raw_json_flashblocks
cargo test --locked --no-default-features --features raw-flashblocks-json,reactive-polling --test raw_json_flashblocks_runtime
cargo clippy --locked --example raw_json_flashblocks_subscriber_acceptance --features raw-flashblocks-json,reactive-ws --no-deps -- -D warnings
cargo +1.90.0 check --locked --lib
cargo +1.90.0 check --locked --lib --no-default-features --features raw-flashblocks-json
cargo bench --no-run --all-features --locked
cargo bench --locked --bench raw_json_flashblocks --no-default-features --features raw-flashblocks-json -- raw_json_flashblocks_application_limit
cargo bench --locked --bench raw_json_flashblocks --no-default-features --features raw-flashblocks-json -- raw_json_flashblocks_near_limit
bash scripts/check-authoring-hygiene.sh
bash scripts/check-security-exceptions.sh
cargo audit --ignore RUSTSEC-2025-0055
cargo package --locked
```

`RUSTSEC-2025-0055` is narrowly ignored because `ark-relations` records
`tracing-subscriber 0.2.25` as an optional lockfile dependency while it remains
absent from `cargo tree --target all --all-features`. The scope script requires
that exact inactive lock entry, rejects any other locked vulnerable version,
and requires every active `tracing-subscriber` to be patched 0.3.20 or newer.
Remove the exception if 0.2.25 ever becomes active or disappears from the lock;
the disappearance intentionally fails the gate until the stale ignore is
removed.

Confirm every third-party `uses:` entry remains pinned to the officially
verified full commit recorded in `SECURITY.md`, not a mutable tag or branch.
The stable and MSRV jobs must use the same pinned `dtolnay/rust-toolchain`
action with explicit `toolchain: stable` and `toolchain: 1.90.0` inputs.
Library CI resolves the checked-in registry-only dependency graph. It must not
introduce sibling source overrides.

Inspect `cargo package --list --locked` and confirm that secrets, local databases,
planning/spec documents, and build output are excluded while consumer
documentation, tests, examples, and benchmarks needed to understand the public
surface are present. The source-only `tests/public_release_surface.rs` audit must
remain excluded because it reads CI and archival planning files that are
intentionally absent from the consumer package. Run authenticated examples or
probes only before this clean-tree preflight, never as part of packaging.
For a raw-profile release candidate, run
`raw_json_flashblocks_subscriber_acceptance` against an independent canonical
WebSocket for 100 matched swaps or five minutes. Record the exact provider,
window, pairing ratio, raw-first latency distribution, duplicate counts, and
canonical-head continuity. The probe must remain opt-in and is not a publishing
side effect.

Record the near-limit frame size, Criterion latency interval, and throughput in
`docs/raw-json-flashblocks-acceptance.md`. Treat the 16 MiB library default as a
defensive compatibility ceiling, not an application recommendation. Verify each
production consumer checks in explicit, source-qualified frame, index,
transaction, and log limits before promotion.

Before publishing a durable subscriber extension, exercise a real multi-block
checkpoint restart through
`ReactiveEngine::preview_durable_resume_position`, the extension's asynchronous
preparation, and `restore_durable_checkpoint`. Do not substitute a manually
assembled one-block resume position; the test must prove the extension consumes
the core's retained canonical history exactly.

## Publish

```bash
cargo publish --locked
git tag -s v0.4.1 -m "Release evm-fork-cache v0.4.1"
git push origin v0.4.1
```

Wait for 0.4.1 to appear in the crates.io index before verifying downstream
extension packages against their final registry-only lockfiles. Publish only after
explicit authorization; preparing or running this checklist is not permission
to publish, tag, or push.

## Optional ingress timing feature

The `ingress-timing` Cargo feature is disabled by default. Without it, new
receipt types, callback configuration, accessors, inline metadata fields and
receipt clockwork are not compiled. Existing canonical and Flashblock timing
APIs keep their prior behavior. Enabling the feature adds only generic data and
hooks; consumers must still explicitly install a bounded receipt lookup. No
exporter, tracing subscriber, global setup or runtime task is installed.

Use `features = ["ingress-timing"]` alongside the existing transport features.
Default/default-off and enabled builds, compile-fail API absence, record-layout
regression and a real consumed-subscription timing fixture are covered in CI.
This is a compatible extension at Alloy's pubsub boundary, not an Alloy-provided
telemetry subsystem. Request Tower layers do not expose unsolicited receipt time.
