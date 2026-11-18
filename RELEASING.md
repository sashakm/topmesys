# Releasing topmesys

Releases are automated with [release-plz](https://release-plz.dev). Configuration lives in
`release-plz.toml`; the pipeline is `.github/workflows/release-plz.yml`.

## How a release happens

1. Every push to `main` opens or updates one **release PR**. It bumps the version in
   `Cargo.toml`, writes the new section of `CHANGELOG.md` from the commits since the last
   tag, and runs cargo-semver-checks against the published API.
2. Review the PR: is the bump right, and does the changelog read well? Edit the PR if
   needed.
3. Merge it. The release job publishes to crates.io, tags `v<version>` and creates the
   GitHub release.

## Commit messages decide the version

The version bump and the changelog come from [conventional commits](https://www.conventionalcommits.org).
release-plz reads **only the first line** of each commit:

| Commit | Before 1.0 | From 1.0 |
|---|---|---|
| `fix: …`, `perf: …`, `refactor: …` | patch | patch |
| `feat: …` | patch | minor |
| `feat!: …`, or a `BREAKING CHANGE:` footer | **minor** | **major** |

Before 1.0, Cargo treats `0.2.0` and `0.2.1` as compatible and upgrades between them
silently. A breaking change released as a patch therefore breaks every dependent on its
next `cargo update`, which is what happened with 0.2.1. cargo-semver-checks in the release
PR now catches API breaks even when a commit message misses the `!`.

Put one change in each commit, or squash-merge PRs under a conventional title. A breaking
change listed on a later line of a multi-line message is invisible to the version
calculation.

## One-time setup

1. **Let Actions open PRs.** Repository settings → Actions → General → Workflow
   permissions: enable *Allow GitHub Actions to create and approve pull requests*.
2. **Optional: `RELEASE_PLZ_TOKEN`.** PRs opened with the default `GITHUB_TOKEN` do not
   trigger other workflows, so CI does not run on the release PR. A fine-grained PAT (or
   a GitHub App token) with *contents* and *pull requests* write access, stored as the
   `RELEASE_PLZ_TOKEN` secret, fixes that.
3. **Trusted publishing.** On crates.io: topmesys → Settings → Trusted Publishing → add
   GitHub, repository `sashakm/topmesys`, workflow `release-plz.yml`. The release job
   authenticates with OIDC (`id-token: write`); no registry token is stored anywhere.

## 0.3.0: re-releasing 0.2.1

0.2.1 contained the breaking consumer API changes (`subscriptions()` and
`handle_event(&Topic, &Delivery)` instead of `consumes()` and `handle_event(Arc<EventMessage>)`)
under a patch number. `Cargo.toml` is already set to 0.3.0, so the next release PR ships
that version with an identical API. After it is published:

1. Move dependents to `topmesys = "0.3"` (jenny: see its `RELEASING.md`).
2. Yank the patch: `cargo yank --version 0.2.1 topmesys`. Existing lockfiles keep working;
   fresh resolution of `"0.2"` falls back to 0.2.0, whose API matches what `^0.2` promises.
