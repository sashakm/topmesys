# topmesys 

[![pipeline status](https://github.com/sashakm/topmesys/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/sashakm/topmesys)
[![coverage](https://codecov.io/gh/sashakm/topmesys/branch/main/graph/badge.svg)](https://codecov.io/gh/sashakm/topmesys)
[![Crates.io](https://img.shields.io/crates/v/topmesys.svg)](https://crates.io/crates/topmesys)

An embeddable topic-based message broker.

## What it does

topmesys is an in-process publish/subscribe event bus for [tokio](https://tokio.rs) applications.
It lets loosely coupled parts of an application exchange messages through hierarchical topics
instead of calling each other directly.

- **Topic-based routing** — messages carry a routing key like `orders.eu.created`; consumers
  subscribe with patterns and only receive matching messages.
- **Flexible patterns** — subscription topics support literal segments (`orders`), wildcards
  (`orders.*.created`), selections (`orders.[eu,us].paid`) and tail matching
  (`orders.*` matches `orders.eu`, `orders.eu.created`, ...).
- **Typestate lifecycle** — invalid states are unrepresentable: an `EventTopic` must be turned
  into a routing key or a subscription pattern before use, and messages can only be sent to an
  `EventBroker<Running>`.
- **Batched, cancel-safe submission** — the `EventEmitter` trait submits single messages or
  batches through channel permits.
- **Graceful shutdown** — buffered messages are drained when the broker is stopped or the
  process receives Ctrl-C.

Matching messages are dispatched concurrently to all subscribed consumers; messages with no
matching consumer are dropped silently.

## Quick start

```rust
use std::sync::Arc;
use tokio::sync::mpsc;
use topmesys::{EventBroker, EventConsumer, EventEmitter, EventMessage, EventSubmission};

// A consumer subscribes to a topic pattern and handles matching events.
#[derive(Debug)]
struct Invoicing {
    topic: String,
}

#[async_trait::async_trait]
impl EventConsumer for Invoicing {
    fn consumes(&self) -> &String {
        &self.topic
    }

    async fn handle_event(&self, event: Arc<EventMessage>) -> anyhow::Result<()> {
        println!("invoicing {}: {:?}", event.topic(), event.content());
        Ok(())
    }
}

// An emitter wraps a sender obtained from the running broker.
#[derive(Debug)]
struct OrderService {
    sender: mpsc::Sender<EventMessage>,
}

#[async_trait::async_trait]
impl EventEmitter for OrderService {
    fn get_sender(&self) -> &mpsc::Sender<EventMessage> {
        &self.sender
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let broker = EventBroker::default();
    broker.add_topic_consumer(Invoicing {
        topic: "orders.*.created".to_string(),
    })?;

    let broker = broker.run()?;
    let orders = OrderService {
        sender: broker.get_sender(),
    };

    let event = EventMessage::new("orders.eu.created", r#"{"order":"A-1001"}"#)?;
    orders.submit_event(EventSubmission::Single(event)).await?;

    broker.stop().await;
    Ok(())
}
```

Complete, runnable scenarios live in [examples/](examples/):

```bash
cargo run --example sensor_hub      # wildcard and selection patterns
cargo run --example order_pipeline  # fan-out to multiple services
```

Benchmarks for topic parsing, subscription matching and end-to-end dispatch:

```bash
cargo bench --bench broker
```

## Development

ℹ️ Make sure just, git-cliff and cargo-llvm-cov are installed:
```bash
cargo install cargo-llvm-cov just git-cliff
```

Common operations related to development and release can be found in the justfile.
For an overview of available recipes, run:
```bash
just
```

## License

Licensed under either of

 * Apache License, Version 2.0
   ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license
   ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
