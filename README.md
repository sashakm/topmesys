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
- **Multi-topic consumers** — a consumer declares several subscriptions, each identified by a
  value of its own `Topic` type (typically an enum), and handles every message knowing which
  subscription it arrived on.
- **Independent subscriptions** — every subscription has its own bounded inbox and worker with
  configurable concurrency; a concurrency of `1` handles messages strictly in order.
- **Retries and dead letters** — failed deliveries are retried with fixed, linear or exponential
  backoff; permanent failures, exhausted retries and overflowing inboxes end up in a
  `DeadLetterSink`.
- **Transport handles** — messages bridged in from NATS, Kafka and the like can carry their
  transport's handle. Consumers can access it, and the broker settles it exactly once with the
  outcome of every subscription, so the bridge can ack, nak or terminate the message upstream.
- **Typestate lifecycle** — invalid states are unrepresentable: an `EventTopic` must be turned
  into a routing key or a subscription pattern before use, and messages can only be sent to an
  `EventBroker<Running>`.
- **Batched, cancel-safe submission** — the `EventEmitter` trait submits single messages or
  batches through channel permits.
- **Graceful shutdown** — stopping the broker drains all buffered messages and waits for
  in-flight deliveries, including pending retries; stopping on Ctrl-C can be opted into with
  `with_ctrl_c_handling()`.

Matching messages are delivered to every matching subscription; messages with no matching
subscription are dropped silently. When a subscription's inbox is full, routing waits for it by
default (`Overflow::Block`), which means a subscription busy retrying holds up all others once its
inbox fills. Subscriptions depending on unreliable systems can use `Overflow::DeadLetter` instead.

## Quick start

```rust
use std::time::Duration;

use tokio::sync::mpsc;
use topmesys::{
    Delivery, EventBroker, EventConsumer, EventEmitter, EventMessage, EventSubmission,
    HandlerError, RetryPolicy, Subscription, Subscriptions,
};

// Identifies the subscriptions of the consumer below.
#[derive(Debug)]
enum OrderTopic {
    Created,
    Cancelled,
}

// A consumer subscribes to topic patterns and handles matching events per subscription.
#[derive(Debug)]
struct Invoicing;

#[async_trait::async_trait]
impl EventConsumer for Invoicing {
    type Topic = OrderTopic;

    fn subscriptions(&self) -> Subscriptions<OrderTopic> {
        Subscriptions::new()
            .on(OrderTopic::Created, "orders.*.created")
            .on(
                OrderTopic::Cancelled,
                Subscription::new("orders.*.cancelled")
                    .with_retry_policy(RetryPolicy::exponential(3, Duration::from_millis(100))),
            )
    }

    async fn handle_event(
        &self,
        topic: &OrderTopic,
        delivery: &Delivery,
    ) -> Result<(), HandlerError> {
        let order = std::str::from_utf8(delivery.message().content())?;
        match topic {
            OrderTopic::Created => println!("invoicing {order}"),
            OrderTopic::Cancelled => println!("voiding the invoice of {order}"),
        }
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
    broker.add_topic_consumer(Invoicing)?;

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

Messages can carry string headers for what is about the event rather than in it, such as an
event id, a correlation id or a W3C `traceparent`. The broker passes them through untouched:

```rust
let event = EventMessage::new("orders.eu.created", r#"{"order":"A-1001"}"#)?
    .with_header("correlation-id", "8f14e45f-ea1a-4a2c-9a1e-2d4c1f0b3a77");
// in a consumer: delivery.message().header("correlation-id")
```

Errors returned from `handle_event` are retried according to the subscription's `RetryPolicy`;
return `HandlerError::permanent(error)` for errors retrying won't fix. Messages a subscription
gives up on are handed to the `DeadLetterSink` set with `EventBroker::with_dead_letter_sink` or
`Subscription::with_dead_letter_sink`, and logged and dropped without one.

### Bridging other messaging systems

A message received from another messaging system can carry the transport's handle. The broker
settles it once, after every subscription the message was routed to finished, e.g. for NATS
JetStream:

```rust
#[derive(Debug)]
struct JetStreamHandle(async_nats::jetstream::Message);

#[async_trait::async_trait]
impl TransportHandle for JetStreamHandle {
    async fn settle(&self, settlement: &Settlement) -> anyhow::Result<()> {
        let kind = if settlement.all_handled() || settlement.is_unrouted() {
            AckKind::Ack
        } else if settlement.any(DeliveryOutcome::Aborted) {
            AckKind::Nak(None)
        } else {
            AckKind::Term
        };
        Ok(self.0.ack_with(kind).await?)
    }

    // Keeps JetStream from redelivering while a subscription waits to retry.
    async fn on_retry(&self, _attempt: u32, _delay: Duration) -> anyhow::Result<()> {
        Ok(self.0.ack_with(AckKind::Progress).await?)
    }
}

let message = EventMessage::new(js_message.subject.as_str(), js_message.payload.clone())?
    .with_transport(JetStreamHandle(js_message));
```

Consumers and dead letter sinks get the handle back with `transport::<JetStreamHandle>()`.

Complete, runnable scenarios live in [examples/](examples/):

```bash
cargo run --example sensor_hub        # wildcard and selection patterns
cargo run --example order_pipeline    # fan-out to multiple services
cargo run --example transport_bridge  # retries, dead letters and transport handles
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
