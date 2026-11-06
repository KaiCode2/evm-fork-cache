# Exact Aero alpha acceptance

Kai approved the exact security and alpha-publication terms on 6 November 2026,
recorded at 11:28:47 UTC, through 8 November 16:00 UTC. The approved terms have
SHA-256 `fc28bcfd5cfa812a7a11e529c1aec607bd853a0c0252574ea441fdc69027b628`.
The complete terms and approval record are in bifi-arbitrage PR #551:
https://github.com/KaiCode2/bifi-arbitrage/pull/551

Only RUSTSEC-2026-0387 on the exact crypto-bigint 0.5.5 registry package is
accepted for these exact alpha publications and isolated qualification. The
inactive tracing-subscriber policy remains separately enforced; all warnings
remain visible. No new vulnerabilities, changed payloads/locks, automatic renewal
or production authority are admitted. Audit reports must be unfiltered and fresh.

The module binds the exact source, archive, payload, lock and workflow. CI may
qualify policy-only changes with current VCS metadata, but that regenerated
archive is not authorized for publication. Publication must use the approved
clean source commit and byte-identical approved archive; --publication-check
verifies this separately. Neither command uploads a package.

No merge, tag, worker-image publication, release promotion, production signing,
deployment or activation is authorized. Public packages remain public after expiry.
