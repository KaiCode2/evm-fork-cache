# Contributing to evm-fork-cache

Thanks for your interest in contributing! This crate is pre-1.0 and developed
against a phased [roadmap](docs/ROADMAP.md). Contributions — bug reports, tests,
docs, examples, and code — are welcome.

## Getting started

```sh
git clone https://github.com/KaiCode2/evm-fork-cache
cd evm-fork-cache
cargo test
```

The crate is a standalone workspace (it has its own `Cargo.lock`) and needs no
network for the default test suite: every integration test builds the cache over
a mocked provider. A handful of examples and benchmarks fork live mainnet state
behind an `RPC_URL` environment variable and are skipped when it is unset.

## The green bar

CI runs the checks below, and every commit on a feature branch is expected to
pass **all** of them. Run them locally before pushing:

```sh
cargo fmt --all --check
cargo clippy --all-targets --no-deps -- -D warnings
# The generic engine must also build and lint cleanly without the protocols feature:
cargo clippy --lib --no-default-features --no-deps -- -D warnings
cargo test
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps
```

A convenience one-liner:

```sh
cargo fmt --all --check && \
cargo clippy --all-targets --no-deps -- -D warnings && \
cargo clippy --lib --no-default-features --no-deps -- -D warnings && \
cargo test && \
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps
```

### MSRV

The minimum supported Rust version is **1.88** (edition 2024), enforced by a
dedicated CI job (`cargo check --lib --locked` on 1.88). Do not use std APIs
newer than 1.88 in the library. Dev-only code (examples, benches, tests) is not
MSRV-constrained.

### Feature configurations

The `protocols` feature (default on) gates DeFi protocol knowledge. The generic
simulation engine must compile and lint with `--no-default-features`. Any new
DeFi-specific surface (protocol storage layouts, pool injection) must be gated
behind `protocols`; generic machinery stays always-on. When you add a public
item behind `#[cfg(feature = "protocols")]`, also add
`#[cfg_attr(docsrs, doc(cfg(feature = "protocols")))]` so docs.rs renders the
feature badge.

## Tests, benchmarks, and examples

- **Tests** live in `tests/` (integration) and inline `#[cfg(test)]` modules
  (unit). Shared offline helpers are in `tests/common/`. Keep tests deterministic
  and network-free; use the stub `StorageBatchFetchFn` helpers for the freshness
  paths. A test should pin a behavior, not merely exercise a code path.
- **Benchmarks** use Criterion and live in `benches/`. Offline benches must stay
  reproducible; RPC-gated benches must `return` early (skip, not fail) when
  `RPC_URL` is unset, so `cargo bench` is offline by default.
- **Examples** live in `examples/`. Offline examples share `examples/support/mock.rs`.
  Each example should explain *what* it shows and *why* it matters, and be listed
  in the README table with its network requirement and level.

## Documentation

- Document every public item. There is no `missing_docs` gate, but
  `cargo doc` runs with `-D warnings`, so broken intra-doc links and malformed
  doc comments fail CI.
- Functions returning `Result` should carry an `# Errors` section; functions that
  can panic should carry a `# Panics` section.
- Prefer runnable doctests; mark network-dependent snippets `no_run` or `ignore`.

## Commits and branches

- Branch from `main` (or the active phase branch). Feature/phase branches follow
  the `phase-N-<topic>` convention.
- Write focused commits with a clear subject line and a body explaining the *why*.
- Update `CHANGELOG.md` under `[Unreleased]` for any user-visible change.

## Reporting issues

Please include the crate version, Rust version, feature flags, and a minimal
reproduction. Known limitations are tracked in
[`docs/KNOWN_ISSUES.md`](docs/KNOWN_ISSUES.md) — check there first.

## License

By contributing, you agree that your contributions will be dual-licensed under
the MIT and Apache-2.0 licenses, as described in the [README](README.md#license).
