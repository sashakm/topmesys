---
name: verify
description: Run topmesys's quality gates for the current change (fmt, clippy, rustdoc, tests with coverage, MSRV, packaging, downstream jenny build) and report the results. Use before declaring any change in this repo done, before committing, and whenever asked to run the tests.
---

# Verify a topmesys change

Run the gates, report only what the output shows, and never call something green
without having seen it pass in this session.

## Always

```sh
cargo fmt
just check   # fmt --check, clippy --all-targets -D warnings, rustdoc -D warnings
just test    # doc tests, then tests and examples under llvm-cov; prints the coverage summary
```

## Depending on the change

| Touched | Also run | Why |
|---|---|---|
| anything `pub` | the `semver-review` command | a break needs a minor bump before 1.0 |
| `Cargo.toml` | `just package`, `just msrv` | crates.io packaging, declared `rust-version` |
| shutdown, routing, retries, settlement | a test that proves the invariant (see `CLAUDE.md`) | these are the guarantees users rely on |
| a hot path (`dispatch`, `match_topic`, `Worker::process`) | `cargo bench` on `main` and on the branch | claim a speed-up only with numbers |
| `EventConsumer`, `Delivery`, `Subscriptions`, `TransportHandle`, `HandlerError` | build jenny against this checkout (below) | jenny implements these traits |

### Downstream check against jenny

In `../jenny-framework`, temporarily add to the root `Cargo.toml`:

```toml
[patch.crates-io]
topmesys = { path = "../topmesys" }
```

then run `cargo check --workspace --all-features --all-targets` there, and remove the
patch again. If the topmesys version moved to a new minor, the requirement in jenny's
`Cargo.toml` must move too, or the patch is silently ignored. Cargo warns "patch was not
used" when that happens.

## Report

One line per gate that ran, pass or fail, with the counts and coverage from the output.
List any gate the change needed but that was skipped, and why.
