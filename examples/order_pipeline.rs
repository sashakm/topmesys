//! An e-commerce order pipeline.
//!
//! Order services publish lifecycle events to `orders.<region>.<status>`
//! topics. Downstream services pick out the slices they care about:
//! - invoicing reacts to newly created orders in any region
//!   (`orders.*.created`)
//! - a compliance audit trail records every event from the EU region
//!   (`orders.eu.*`)
//! - analytics ingests the whole order stream (`orders.*`)
//!
//! Run with: `cargo run --example order_pipeline`

use std::sync::Arc;

use tokio::sync::mpsc;
use topmesys::{EventBroker, EventConsumer, EventEmitter, EventMessage, EventSubmission};

#[derive(Debug)]
struct Service {
    name: &'static str,
    topic: String,
}

#[async_trait::async_trait]
impl EventConsumer for Service {
    fn consumes(&self) -> &str {
        &self.topic
    }

    async fn handle_event(&self, event: Arc<EventMessage>) -> anyhow::Result<()> {
        println!(
            "[{:>10}] {} -> {}",
            self.name,
            event.topic(),
            std::str::from_utf8(event.content())?
        );
        Ok(())
    }
}

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
    broker.add_topic_consumer(Service {
        name: "invoicing",
        topic: "orders.*.created".to_string(),
    })?;
    broker.add_topic_consumer(Service {
        name: "compliance",
        topic: "orders.eu.*".to_string(),
    })?;
    broker.add_topic_consumer(Service {
        name: "analytics",
        topic: "orders.*".to_string(),
    })?;

    let broker = broker.run()?;
    let orders = OrderService {
        sender: broker.get_sender(),
    };

    // A single event...
    orders
        .submit_event(EventSubmission::Single(EventMessage::new(
            "orders.eu.created",
            r#"{"order":"A-1001","total":"129.90"}"#,
        )?))
        .await?;

    // ...and a batch covering the rest of the lifecycle.
    let batch = [
        ("orders.us.created", r#"{"order":"B-2002"}"#),
        ("orders.eu.paid", r#"{"order":"A-1001"}"#),
        ("orders.eu.shipped", r#"{"order":"A-1001"}"#),
        ("orders.us.cancelled", r#"{"order":"B-2002"}"#),
    ]
    .into_iter()
    .map(|(topic, payload)| EventMessage::new(topic, payload))
    .collect::<Result<EventSubmission, _>>()?;
    orders.submit_event(batch).await?;

    // Stopping drains all buffered messages and waits for in-flight handlers to finish.
    broker.stop().await;

    Ok(())
}
