#!/usr/bin/env bash
# Print the latest crates.io version of each crate name passed as an argument.
# Runs from an empty directory so `cargo info` reports the registry's newest release
# rather than the version locked in the current workspace.
set -uo pipefail
empty="$(mktemp -d)"
trap 'rm -rf "$empty"' EXIT
cd "$empty"
for name in "$@"; do
  version="$(cargo info "$name" 2>/dev/null | sed -n 's/^version: \([^ ]*\).*/\1/p' | head -1)"
  printf '%-32s %s\n' "$name" "${version:-?}"
done
