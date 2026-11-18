---
name: update-deps
description: Update topmesys's dependencies to their latest releases, including breaking majors, fix the fallout and keep the declared MSRV honest. Use when asked to update, bump or upgrade dependencies, or when Dependabot raises a major-version PR.
---

# Update topmesys's dependencies

## 1. What is out of date

```sh
grep -oE '^[a-z_0-9-]+ =' Cargo.toml | sed 's/ =//' \
  | grep -vE '^(name|description|repository|keywords|license|readme|homepage|exclude|version|edition|rust-version|harness)$' \
  | sort -u | xargs bash .claude/skills/update-deps/latest.sh
```

A prerelease (dashmap `7.0.0-rc2`) is not a target; stay on the newest stable.

## 2. Bump

- Raise the requirements in `Cargo.toml`, `[dependencies]` and `[dev-dependencies]`
  alike. The `tokio` entries in both sections should name the same version.
- `cargo update`, then `cargo check --all-targets`, and fix each breakage. Read the
  changelog of every major instead of guessing.
- A dependency whose types appear in the public API is part of topmesys's API: `tokio`
  (`mpsc::Sender` in `EventEmitter` and `EventBroker::get_sender`), `bytes` (`Bytes` in
  `EventMessage`), `anyhow` (`HandlerError`, `TransportHandle`). A major bump of one of
  those is a breaking change for topmesys, so commit it as `chore(deps)!: …`.

## 3. MSRV

```sh
python3 .claude/skills/update-deps/msrv_floor.py
```

The declared `rust-version` is the higher of that "published crates" number and what
the code itself uses: let-chains need 1.88. If it rises, update `rust-version` in
`Cargo.toml`, the `msrv` recipe in `justfile` and the `msrv` job in
`.github/workflows/ci.yml`, then run `just msrv`.

## 4. Verify and commit

Run the `verify` skill, including the downstream jenny check when tokio, bytes or anyhow
moved. Commit as `chore(deps): …`.
