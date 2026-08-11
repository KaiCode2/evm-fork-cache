#!/usr/bin/env bash
set +x
set -euo pipefail

cd "$(dirname "$0")/.."

tracing_policy_error=""

is_patched_tracing_subscriber_version() {
  local version="$1"
  if [[ ! "$version" =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]; then
    return 1
  fi

  local major="${BASH_REMATCH[1]}"
  local minor="${BASH_REMATCH[2]}"
  local patch="${BASH_REMATCH[3]}"
  ((major > 0 || minor > 3 || (minor == 3 && patch >= 20)))
}

validate_tracing_subscriber_policy() {
  local locked_versions="$1"
  local active_versions="$2"
  local version
  local ignored_lock_entries=0
  local active_entries=0
  tracing_policy_error=""

  while IFS= read -r version; do
    [[ -n "$version" ]] || continue
    if [[ "$version" == "0.2.25" ]]; then
      ((ignored_lock_entries += 1))
    elif ! is_patched_tracing_subscriber_version "$version"; then
      tracing_policy_error="Cargo.lock contains another tracing-subscriber version covered by RUSTSEC-2025-0055"
      return 1
    fi
  done <<<"$locked_versions"

  if ((ignored_lock_entries != 1)); then
    tracing_policy_error="Cargo.lock must contain exactly one tracing-subscriber 0.2.25 entry until the advisory ignore is removed"
    return 1
  fi

  while IFS= read -r version; do
    [[ -n "$version" ]] || continue
    ((active_entries += 1))
    if ! is_patched_tracing_subscriber_version "$version"; then
      tracing_policy_error="the active graph contains tracing-subscriber below patched version 0.3.20"
      return 1
    fi
  done <<<"$active_versions"

  if ((active_entries == 0)); then
    tracing_policy_error="the active tracing-subscriber version set is unexpectedly empty"
    return 1
  fi
}

assert_tracing_policy_fixtures() {
  local valid_locked
  valid_locked="$(printf '%s\n' '0.2.25' '0.3.23')"

  if ! validate_tracing_subscriber_policy "$valid_locked" "0.3.23"; then
    echo "Internal tracing-subscriber policy fixture rejected the reviewed shape." >&2
    exit 1
  fi
  if validate_tracing_subscriber_policy "0.3.23" "0.3.23"; then
    echo "Internal tracing-subscriber policy fixture accepted a stale advisory ignore." >&2
    exit 1
  fi
  if validate_tracing_subscriber_policy \
    "$(printf '%s\n' '0.2.25' '0.3.19' '0.3.23')" "0.3.23"
  then
    echo "Internal tracing-subscriber policy fixture accepted another vulnerable lock entry." >&2
    exit 1
  fi
  if validate_tracing_subscriber_policy "$valid_locked" "0.2.25"; then
    echo "Internal tracing-subscriber policy fixture accepted an active vulnerable version." >&2
    exit 1
  fi
  if validate_tracing_subscriber_policy "$valid_locked" ""; then
    echo "Internal tracing-subscriber policy fixture accepted an empty active set." >&2
    exit 1
  fi
}

assert_tracing_policy_fixtures

# RUSTSEC-2025-0055 is advisory-wide, so prove the complete active version set
# is patched and that 0.2.25 is the only vulnerable locked version. Requiring
# the exact inactive entry to remain makes its removal fail closed: the ignore
# must be deleted instead of silently becoming stale.
locked_tracing_versions="$(
  awk '
    /^\[\[package\]\]$/ {
      if (name == "tracing-subscriber") {
        print version
      }
      name = ""
      version = ""
      next
    }
    /^name = / {
      value = $0
      sub(/^name = "/, "", value)
      sub(/"$/, "", value)
      name = value
      next
    }
    /^version = / {
      value = $0
      sub(/^version = "/, "", value)
      sub(/"$/, "", value)
      version = value
      next
    }
    END {
      if (name == "tracing-subscriber") {
        print version
      }
    }
  ' Cargo.lock | sort
)"
active_dependency_tree="$(
  cargo tree --locked --all-features --target all --prefix none
)"
active_tracing_versions="$(
  printf '%s\n' "$active_dependency_tree" \
    | awk '$1 == "tracing-subscriber" { sub(/^v/, "", $2); print $2 }' \
    | sort -u
)"

if ! validate_tracing_subscriber_policy \
  "$locked_tracing_versions" "$active_tracing_versions"
then
  echo "RUSTSEC-2025-0055 scope check failed: $tracing_policy_error." >&2
  echo "Locked tracing-subscriber versions:" >&2
  printf '%s\n' "$locked_tracing_versions" >&2
  echo "Active tracing-subscriber versions:" >&2
  printf '%s\n' "$active_tracing_versions" >&2
  exit 1
fi

inactive_tracing="$({
  cargo tree --locked --all-features \
    -i tracing-subscriber@0.2.25 --target all --prefix none 2>/dev/null
} || true)"
if [[ -n "$inactive_tracing" ]]; then
  echo "RUSTSEC-2025-0055 is no longer confined to an inactive lock entry." >&2
  echo "$inactive_tracing" >&2
  exit 1
fi

# bincode 1 is retained deliberately for compatibility with the crate's
# versioned on-disk cache formats. It must remain a direct dependency of this
# crate only; a new consumer requires a fresh migration/security decision.
bincode_graph="$({
  cargo tree --locked -i bincode@1.3.3 --target all --prefix depth
} | sed -E 's# \(/[^)]*\)$##; s# \(\*\)$##')"
expected_bincode_graph="$(printf '%s\n' \
  '0bincode v1.3.3' \
  '1evm-fork-cache v0.4.0-alpha.4')"
if [[ "$bincode_graph" != "$expected_bincode_graph" ]]; then
  echo "The accepted bincode 1 compatibility scope changed." >&2
  echo "Expected:" >&2
  echo "$expected_bincode_graph" >&2
  echo "Observed:" >&2
  echo "$bincode_graph" >&2
  exit 1
fi

# derivative is tolerated only as an unreachable lockfile entry.
inactive_derivative="$({
  cargo tree --locked -i derivative@2.2.0 --target all --prefix none 2>/dev/null
} || true)"
if [[ -n "$inactive_derivative" ]]; then
  echo "Unmaintained derivative 2.2.0 became reachable." >&2
  echo "$inactive_derivative" >&2
  exit 1
fi

# paste is an active transitive procedural macro. Keep its immediate reverse
# dependency set pinned so a new path cannot inherit this release decision.
paste_graph="$({
  cargo tree --locked -i paste@1.0.15 --target all --prefix depth --depth 1
} | sed -E 's# \(/[^)]*\)$##; s# \(\*\)$##')"
expected_paste_graph="$(printf '%s\n' \
  '0paste v1.0.15 (proc-macro)' \
  '1alloy-primitives v1.6.1' \
  '1ark-ff v0.5.0' \
  '1syn-solidity v1.6.1')"
if [[ "$paste_graph" != "$expected_paste_graph" ]]; then
  echo "The accepted paste dependency scope changed." >&2
  echo "Expected:" >&2
  echo "$expected_paste_graph" >&2
  echo "Observed:" >&2
  echo "$paste_graph" >&2
  exit 1
fi

echo "Security advisory and unmaintained-dependency scopes match policy."
