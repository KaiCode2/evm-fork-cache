#!/usr/bin/env bash
set -euo pipefail

release_base="1e5bbe854e781c8c7a161bf158c2b645a2fe75c0"
terms=(
  "$(printf '%b' '\x63\x6f\x64\x65\x78')"
  "$(printf '%b' '\x63\x68\x61\x74\x67\x70\x74')"
  "$(printf '%b' '\x6f\x70\x65\x6e\x61\x69')"
  "$(printf '%b' '\x63\x6c\x61\x75\x64\x65')"
  "$(printf '%b' '\x63\x6f\x70\x69\x6c\x6f\x74')"
  "$(printf '%b' '\x67\x65\x6d\x69\x6e\x69')"
  "$(printf '%b' '\x77\x69\x6e\x64\x73\x75\x72\x66')"
  "$(printf '%b' '\x61\x69\x64\x65\x72')"
  "$(printf '%b' '\x64\x65\x76\x69\x6e')"
  "$(printf '%b' '\x63\x6f\x64\x65\x69\x75\x6d')"
)
pattern="$(IFS='|'; printf '%s' "${terms[*]}")"

if ! git cat-file -e "${release_base}^{commit}"; then
  echo "release-base commit is unavailable; fetch complete history" >&2
  exit 1
fi

if git grep -IinE "${pattern}" -- . ':!scripts/check-authoring-hygiene.sh'; then
  echo "tracked release content contains a prohibited attribution term" >&2
  exit 1
fi

if git log "${release_base}..HEAD" --format='%H%n%s%n%b' | grep -inE "${pattern}"; then
  echo "release-delta commit metadata contains a prohibited attribution term" >&2
  exit 1
fi

branch="$(git branch --show-current)"
if printf '%s\n' "${branch}" | grep -inE "${pattern}"; then
  echo "current branch contains a prohibited attribution term" >&2
  exit 1
fi
