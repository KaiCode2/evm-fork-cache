# Security Policy

`evm-fork-cache` is a forked-EVM simulation engine used in search / MEV /
backtesting pipelines, where a correctness bug can have direct financial
consequences for downstream users. Security reports are taken seriously.

## Supported versions

This crate is **pre-1.0** and under active development. Only the latest published
`0.x` release line receives security fixes; there is no back-porting to older
`0.x` versions before 1.0. See [`CHANGELOG.md`](CHANGELOG.md) for what shipped in
each release.

| Version | Supported |
| ------- | --------- |
| latest `0.x` | ✅ |
| older `0.x`  | ❌ |

## Reporting a vulnerability

**Please do not open a public issue for security-sensitive reports.**

Report privately via GitHub's
[private vulnerability reporting](https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing-information-about-vulnerabilities/privately-reporting-a-security-vulnerability)
on this repository: open the **Security** tab → **Report a vulnerability**. If
that channel is unavailable, open a minimal public issue asking for a private
contact (without disclosing details) and a maintainer will follow up.

Please include:

- the crate version, Rust version, and feature flags;
- a description of the issue and its impact;
- a minimal reproduction if possible.

You can expect an initial acknowledgement within a few business days. Once a fix
is available it will be released and the advisory published, crediting the
reporter unless anonymity is requested.

## Scope and known limitations

This crate exposes deliberate, documented escape hatches that **bypass its safety
invariants** — notably `unchecked_blockchain_db()` / `unchecked_backend()` (which
sidestep the copy-on-write snapshot invalidation funnel) and the freshness model's
documented reconciliation scope (storage slots only, not account-level state).
These behaviors and their correct usage are described in
[`docs/KNOWN_ISSUES.md`](docs/KNOWN_ISSUES.md). Misuse of a documented escape hatch
is a usage error, not a vulnerability; a way to violate a documented invariant
*without* using an escape hatch is in scope. When in doubt, report it.

Durable checkpoints are integrity-checked for accidental corruption, not
authenticated. Their directory must be writable only by the service identity,
and exactly one process may own a checkpoint path. Same-path writers are ordered
inside one process, but filesystem rename cannot coordinate independent
processes or defend an attacker-controlled parent directory. The final filename
is replaced as a directory entry (a destination symlink is not followed), and
atomic durable saves currently require Unix; unsupported targets fail closed
with `DurableCheckpointError::AtomicReplaceUnsupported`.

## Dependency audit policy

CI audits the locked dependency graph with RustSec. Before the audit,
`scripts/check-security-exceptions.sh` verifies that every accepted advisory or
unmaintained dependency remains inside the exact scope reviewed for this
release. A changed reverse-dependency path fails the build and requires a new
decision; an exception is never permission to ignore a newly reachable issue.

The current graph has one ignored vulnerability advisory:

- `RUSTSEC-2025-0055` affects `tracing-subscriber` 0.2.25. That version is an
  unreachable lockfile entry. Because cargo-audit ignores the advisory by ID,
  the scope script checks every locked and active `tracing-subscriber` version:
  0.2.25 must remain the only vulnerable lock entry and stay unreachable, while
  every all-feature/all-target active version must be patched 0.3.20 or newer.
  The gate also fails if 0.2.25 disappears so the now-stale ignore must be
  removed.

RustSec also reports three unmaintained crates. They are not vulnerability
advisories, but their disposition is checked on every release:

- `bincode` 1.3.3 remains a direct dependency because the crate's already
  versioned binary cache formats use its encoding. Replacing it requires an
  explicit format migration rather than silently making existing caches
  unreadable. No additional package may acquire this dependency under this
  acceptance.
- `derivative` 2.2.0 is an unreachable lockfile entry and is accepted only
  while it remains unreachable.
- `paste` 1.0.15 is an active transitive procedural macro through the pinned
  Alloy/Arkworks graph. It is not called by this crate at runtime. Its immediate
  reverse-dependency set is pinned by the scope check while upstream migration
  is tracked.

Run `scripts/check-security-exceptions.sh` and `cargo audit --ignore
RUSTSEC-2025-0055` before every release. Remove an exception as soon as its
locked entry or upstream constraint disappears.

Every third-party CI action is pinned to an immutable full commit SHA. Adjacent
comments retain the reviewed human-readable upstream ref:

| Action | Reviewed ref | Pinned commit |
| --- | --- | --- |
| [`actions/checkout`](https://github.com/actions/checkout/releases/tag/v4.4.0) | `v4.4.0` | `11d5960a326750d5838078e36cf38b85af677262` |
| [`dtolnay/rust-toolchain`](https://github.com/dtolnay/rust-toolchain/commit/4cda84d5c5c54efe2404f9d843567869ab1699d4) | `stable` | `4cda84d5c5c54efe2404f9d843567869ab1699d4` |
| [`Swatinem/rust-cache`](https://github.com/Swatinem/rust-cache/releases/tag/v2.9.1) | `v2.9.1` | `c19371144df3bb44fab255c43d04cbc2ab54d1c4` |

The stable and MSRV jobs use the same reviewed toolchain-action commit and pass
their requested toolchain explicitly. Updating any action requires verifying
the new upstream ref and full commit before changing the pin.
