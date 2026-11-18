#!/usr/bin/env bash
# PreToolUse guard: refuses hand edits to files a tool owns. Exit code 2 blocks the
# call and hands the message to Claude.
set -euo pipefail
path="$(jq -r '.tool_input.file_path // empty')"
case "$(basename "$path")" in
  CHANGELOG.md)
    echo "CHANGELOG.md is written by release-plz from conventional commits. Fix the commit message, or edit the release PR." >&2
    exit 2 ;;
  Cargo.lock)
    echo "Cargo.lock is maintained by cargo. Run \`cargo update\` (or \`cargo update -p <crate>\`) instead of editing it." >&2
    exit 2 ;;
esac
exit 0
