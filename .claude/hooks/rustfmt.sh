#!/usr/bin/env bash
# PostToolUse: formats a Rust file right after it is written, so formatting never fails
# `just check`. Never blocks; a file that does not parse is left for the compiler.
set -uo pipefail
path="$(jq -r '.tool_input.file_path // empty')"
[[ "$path" == *.rs && -f "$path" ]] || exit 0
rustfmt --edition 2024 --quiet "$path" 2>/dev/null || true
exit 0
