# topmesys

An embeddable, topic-based publish/subscribe broker for tokio applications. A single
library crate, published on crates.io and used by `../jenny-framework` as its in-process
event bus. Public API stability matters more here than anywhere else in the two repos,
because downstream crates break on it.

## Layout

| File | Holds |
|---|---|
| `src/lib.rs` | `EventTopic` (typestate: `Init` → `RoutingKey` / `Pattern`), `TopicSegment` matching, `EventBroker` (typestate: `Stopped` / `Running`), `EventMessage`, `EventEmitter`, `EventConsumer`, and most tests |
| `src/subscription.rs` | `Subscription`, `Subscriptions<T>`, `Overflow`; the internal `Subscriber`, `Inbox` and per-subscription `Worker` |
| `src/delivery.rs` | `Delivery` (one message on one subscription) and `HandlerError` (transient vs permanent) |
| `src/retry.rs` | `RetryPolicy`, `Backoff` |
| `src/dead_letter.rs` | `DeadLetterSink`, `DeadLetter`, `DeadLetterReason` |
| `src/transport.rs` | `TransportHandle`, `Settlement`, `DeliveryOutcome`; the internal `Settler` |
| `examples/` | runnable usage (`order_pipeline`, `sensor_hub`, `transport_bridge`) |
| `benches/broker.rs` | criterion benchmarks |

Message flow: emitter → bounded broker channel → event loop matches topic patterns →
each matching subscription's bounded inbox → its worker (concurrency-limited, retries,
dead letters) → the consumer's `handle_event`. A message carrying a `TransportHandle` is
settled once, after its last delivery finishes.

## Invariants to keep

- **Typestates make misuse unrepresentable**: a topic must become a routing key or a
  pattern before use, and only a `Running` broker hands out senders. Do not add
  runtime-checked shortcuts around them.
- **Graceful stop loses nothing it accepted**: `stop()` drains the channel, closes the
  inboxes and waits for every worker, including deliveries waiting on a retry. Any change
  to shutdown needs a test that proves it.
- **A panicking handler is a permanent failure**, never a dead worker or a lost message.
- **Settlement happens exactly once per message**, including for aborted deliveries
  (`Delivery`'s `Drop`).
- **Topics are lowercased on creation**; matching is case-insensitive by construction.

## Commands

```sh
just check     # fmt, clippy --all-targets -D warnings, rustdoc -D warnings
just test      # doc tests + all tests and examples under llvm-cov
just msrv      # cargo +1.88 check, the declared rust-version
just package   # package + verify as crates.io will see it
cargo bench    # criterion; compare against main before claiming a speed-up
```

## Semver

Anything `pub` is API, including trait method signatures, enum variants and the generic
parameters of public types. Before 1.0 a breaking change is a **minor** bump (0.3 → 0.4)
and must be marked: `feat!:`/`fix!:` in the commit header or a `BREAKING CHANGE:` footer.
0.2.1 shipped a breaking API change as a patch and broke jenny on its next
`cargo update`. Run the `semver-review` command before committing API changes;
release-plz's cargo-semver-checks run in the release PR is the backstop, not the plan.

Prefer additive changes: a new builder method over a new required parameter, a defaulted
trait method over a new required one, `#[non_exhaustive]` on enums that will grow.

## Releasing and commits

Conventional commits, one change per commit: release-plz reads only the first line.
Pushing to `main` updates the release PR; merging it publishes, once a reviewer approves
the `release` environment. Never run
`cargo publish`, `cargo yank` or `cargo owner` (the project settings deny them), and never
edit `CHANGELOG.md` by hand. Details, including the pending 0.2.1 yank: `RELEASING.md`.

## Downstream

jenny (`../jenny-framework`) implements `EventConsumer` in `jenny-messaging` (`NatsGateway`),
`jenny-server` (`JennyServer::subscribe`), its `examples/basic` and its CLI template
`crates/cargo-jenny/templates/project/events.rs.tmpl`. After an API change, check those
still compile against a path dependency before releasing.
