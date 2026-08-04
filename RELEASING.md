# Releasing

`evm-fork-cache` 0.4.0-alpha.2 is the second prerelease in the Flashblocks
compatibility set. Publish `alloy-transport-balancer 0.3.0-alpha.2` first, then
publish this crate before any extension crate that declares
`evm-fork-cache = "0.4.0-alpha.2"`, including `evm-amm-state 0.3.0-alpha.2` and
the remote/Hybrid subscriber packages.
No release step is automatic: use clean, reviewed commits and never publish
from a credential-bearing working tree.

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
cargo clippy --locked --all-targets --no-default-features --features reactive-polling --no-deps -- -D warnings
cargo test --locked --no-default-features --features reactive-polling
cargo +1.90.0 check --locked --lib
cargo bench --no-run --all-features --locked
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
Confirm the workflow's transport checkout still names the reviewed exact
candidate commit recorded in `SECURITY.md`; a moving branch is not an acceptable
substitute.

Inspect `cargo package --list --locked` and confirm that secrets, local databases,
planning/spec documents, and build output are excluded while consumer
documentation, tests, examples, and benchmarks needed to understand the public
surface are present. Run authenticated examples or probes only before this
clean-tree preflight, never as part of packaging.

Before publishing a durable subscriber extension, exercise a real multi-block
checkpoint restart through
`ReactiveEngine::preview_durable_resume_position`, the extension's asynchronous
preparation, and `restore_durable_checkpoint`. Do not substitute a manually
assembled one-block resume position; the test must prove the extension consumes
the core's retained canonical history exactly.

## Publish

```bash
cargo publish --locked
git tag -s v0.4.0-alpha.2 -m "Release evm-fork-cache v0.4.0-alpha.2"
git push origin v0.4.0-alpha.2
```

Wait for 0.4.0-alpha.2 to appear in the crates.io index before removing sibling path
dependencies and verifying downstream extension packages. Publish only after
explicit authorization; preparing or running this checklist is not permission
to publish, tag, or push.
