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
