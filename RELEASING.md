# Releasing

`evm-fork-cache` is published after `alloy-transport-balancer` and before the
AMM state and search crates. The current 0.3.0 release intentionally uses a new
minor version because public configuration structs gained required fields.

## Preflight

```bash
cargo fmt --all -- --check
cargo test --all-targets --all-features
cargo test --doc --all-features
cargo clippy --all-targets --all-features --no-deps -- -D warnings
cargo clippy --all-targets --no-default-features --features reactive-polling --no-deps -- -D warnings
cargo test --no-default-features --features reactive-polling
cargo build --no-default-features
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features
cargo bench --no-run --all-features
cargo bench --bench reactive_routing --features reactive
cargo +1.88 check --lib --locked
cargo package --locked
cargo audit --ignore RUSTSEC-2025-0055
```

`RUSTSEC-2025-0055` is narrowly ignored because `ark-relations` records
`tracing-subscriber 0.2.25` as an optional lockfile dependency while it remains
absent from `cargo tree --target all --all-features`. Remove the exception if
that version ever becomes active, or when the upstream metadata no longer
records it.

Inspect `cargo package --list` and confirm that planning/spec documents remain
excluded while consumer documentation, tests, examples, and benchmarks needed
to understand the public surface are present.

## Publish

```bash
cargo publish --locked
git tag -s v0.3.0 -m "Release evm-fork-cache v0.3.0"
git push origin v0.3.0
```

Wait for 0.3.0 to appear in the crates.io index before packaging
`evm-amm-state`.
