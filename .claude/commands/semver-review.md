---
description: Classify the public API changes since the last release tag as breaking or additive, and say which version bump and commit marker they need
argument-hint: "[base ref, default: latest v* tag]"
allowed-tools: Bash(git diff:*), Bash(git log:*), Bash(git tag:*), Bash(git describe:*), Bash(cargo semver-checks:*), Bash(cargo doc:*), Read, Grep, Glob
---

Compare topmesys's public API between `$ARGUMENTS` (if empty: the newest `v*` tag from
`git describe --tags --abbrev=0 --match 'v*'`) and the working tree.

1. If `cargo semver-checks` is installed, run
   `cargo semver-checks --baseline-rev <base>` and use its findings as the starting
   point. If it is not installed, say so and continue by hand. Do not install it without
   asking.
2. Read `git diff <base> -- src/` and list every change to an item reachable from the
   crate root: `pub` types, fields, functions, methods, trait items and their
   signatures, enum variants, generic parameters and bounds, re-exports, and trait
   impls on public types (adding `Clone`, removing `Default`).
3. Classify each one:
   - **Breaking**: removed or renamed items, changed signatures, new required trait
     items, new variants on an enum that is not `#[non_exhaustive]`, tighter bounds,
     removed trait impls, and behaviour changes users would have to adapt to (a default
     that changed, a guarantee that no longer holds).
   - **Additive**: new items, new defaulted trait methods, new variants on a
     `#[non_exhaustive]` enum, looser bounds.
4. Say what this means for the release. Current version: `Cargo.toml`. Before 1.0 a
   break needs a **minor** bump (0.3.x to 0.4.0) and a commit header with `!`, or a
   `BREAKING CHANGE:` footer. Additive changes are a patch.
5. For each breaking change, suggest an additive alternative if one exists, such as a
   builder method instead of a new parameter or a defaulted trait method.

Report as a table (item, change, breaking or additive, file:line), then the verdict in one
line, e.g. "Breaking: release as 0.4.0, commit as `feat!: …`". Do not edit files.
