use std::{fs, path::Path};

fn read(path: &str) -> String {
    fs::read_to_string(path).unwrap_or_else(|err| panic!("failed to read {path}: {err}"))
}

#[test]
fn manifest_no_longer_defines_protocols_feature_or_protocol_benchmarks() {
    let manifest = read("Cargo.toml");

    for forbidden in [
        "protocols",
        "name = \"storage_keys\"",
        "benches/storage_keys.rs",
    ] {
        assert!(
            !manifest.contains(forbidden),
            "Cargo.toml should not expose protocol-specific surface: {forbidden}"
        );
    }
}

#[test]
fn protocol_modules_are_not_part_of_the_core_crate_surface() {
    for path in [
        "src/events/uniswap_v3.rs",
        "src/cache/storage_keys.rs",
        "src/cache/tick_snapshot.rs",
        "tests/storage_keys.rs",
        "tests/event_ground_truth.rs",
        "benches/storage_keys.rs",
    ] {
        assert!(
            !Path::new(path).exists(),
            "{path} belongs in the protocol adapter crate"
        );
    }

    for path in ["src/lib.rs", "src/events/mod.rs", "src/cache/mod.rs"] {
        let text = read(path);
        for forbidden in [
            "cfg(feature = \"protocols\")",
            "doc(cfg(feature = \"protocols\"))",
            "UniswapV3Decoder",
            "UniswapV3Layout",
            "tick_snapshot",
            "storage_keys",
            "inject_v2_pool_metadata",
            "inject_v3_",
        ] {
            assert!(
                !text.contains(forbidden),
                "{path} still contains protocol-specific surface: {forbidden}"
            );
        }
    }
}

#[test]
fn immutable_cache_is_generic_token_decimals_only() {
    let metadata = read("src/cache/metadata.rs");

    assert!(
        metadata.contains("IMMUTABLE_CACHE_VERSION: u32 = 2"),
        "metadata cache format should be bumped after removing old pool metadata fields"
    );
    assert!(
        metadata.contains("token_decimals"),
        "token decimals remain the generic immutable cache payload"
    );

    for forbidden in [
        "V2PoolMetadata",
        "V3PoolMetadata",
        "BalancerPoolMetadata",
        "v2_pools",
        "v3_pools",
        "balancer_pools",
        "get_v2_pool",
        "set_v2_pool",
        "get_v3_pool",
        "set_v3_pool",
        "get_balancer_pool",
        "set_balancer_pool",
        "tick_snapshot_cache_path",
    ] {
        assert!(
            !metadata.contains(forbidden),
            "immutable cache should not retain protocol metadata: {forbidden}"
        );
    }
}

#[test]
fn release_docs_and_ci_do_not_advertise_removed_protocol_feature() {
    for path in [
        "README.md",
        "CHANGELOG.md",
        "CONTRIBUTING.md",
        ".github/workflows/ci.yml",
        "docs/KNOWN_ISSUES.md",
        "docs/ROADMAP.md",
    ] {
        let text = read(path);
        for forbidden in [
            "protocols feature",
            "feature = \"protocols\"",
            "default = [\"protocols\"]",
            "--no-default-features",
            "non-`protocols`",
            "UniswapV3Decoder",
            "inject_v3_",
            "V3 tick snapshot",
            "storage_keys",
        ] {
            assert!(
                !text.contains(forbidden),
                "{path} still advertises removed protocol surface: {forbidden}"
            );
        }
    }
}

#[test]
fn phase_specs_are_marked_as_archival_after_protocol_extraction() {
    for path in [
        "docs/phase-2-spec.md",
        "docs/phase-3-spec.md",
        "docs/phase-4-spec.md",
        "docs/phase-5-spec.md",
    ] {
        let text = read(path);
        let top = text.lines().take(10).collect::<Vec<_>>().join("\n");

        assert!(
            top.contains("Archival pre-release implementation note"),
            "{path} should clearly mark the phase spec as archival"
        );
        assert!(
            top.contains("protocol adapter surface"),
            "{path} should explain that protocol adapters were extracted before release"
        );
        assert!(
            top.contains("evm-amm-state"),
            "{path} should point protocol-specific state tracking to evm-amm-state"
        );
    }
}

#[test]
fn generic_storage_purge_api_uses_contract_terminology() {
    let cache = read("src/cache/mod.rs");

    for required in [
        "has_contract_storage",
        "contract_storage_slot_count",
        "purge_contract_storage",
        "purge_contract_slots",
    ] {
        assert!(
            cache.contains(required),
            "cache API should expose generic contract terminology: {required}"
        );
    }

    for path in [
        "src/cache/mod.rs",
        "src/state_update.rs",
        "README.md",
        "CHANGELOG.md",
        "docs/ROADMAP.md",
        "docs/KNOWN_ISSUES.md",
        "tests/cache_state.rs",
        "tests/state_update.rs",
        "tests/freshness.rs",
        "examples/state_update_apply.rs",
    ] {
        let text = read(path);
        for forbidden in [
            "has_pool_storage",
            "pool_storage_slot_count",
            "purge_pool_storage",
            "purge_pool_slots",
            "hot pool state",
        ] {
            assert!(
                !text.contains(forbidden),
                "{path} still exposes pool-oriented cache wording: {forbidden}"
            );
        }
    }
}
