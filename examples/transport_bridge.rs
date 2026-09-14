//! Bridging an external message queue into the broker.
//!
//! A stand-in queue client plays the part of a networked transport like NATS JetStream or
//! Kafka: every message it hands out comes with a handle to acknowledge it. The bridge attaches
//! that handle to the submitted message, and the broker settles it once, after all subscriptions
//! finished with the message:
//! - billing handles settled and failed payments on separate subscriptions of one consumer
//!   (`payments.*.settled`, `payments.*.failed`)
//! - reversing a failed payment calls a flaky ledger service, so that subscription retries with
//!   exponential backoff while keeping the queue message alive
//! - malformed payments are dead-lettered right away, unroutable ones are acknowledged
//!
//! Run with: `cargo run --example transport_bridge`

use std::{
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};

use tokio::sync::mpsc;
use topmesys::{
    DeadLetter, DeadLetterSink, Delivery, DeliveryOutcome, EventBroker, EventConsumer,
    EventEmitter, EventMessage, EventSubmission, HandlerError, RetryPolicy, Settlement,
    Subscription, Subscriptions, TransportHandle,
};

/// What a queue client typically hands out per message, next to the payload.
#[derive(Debug)]
struct QueueMessage {
    id: u64,
}

impl QueueMessage {
    async fn ack(&self) {
        println!("[queue]    #{} ack", self.id);
    }

    async fn nak(&self) {
        println!("[queue]    #{} nak, redeliver later", self.id);
    }

    async fn term(&self) {
        println!("[queue]    #{} term, never redeliver", self.id);
    }

    async fn in_progress(&self) {
        println!("[queue]    #{} in progress, extend deadline", self.id);
    }
}

/// Implementing topmesys' trait for the client's type requires a newtype.
#[derive(Debug)]
struct QueueHandle(QueueMessage);

#[async_trait::async_trait]
impl TransportHandle for QueueHandle {
    async fn settle(&self, settlement: &Settlement) -> anyhow::Result<()> {
        if settlement.all_handled() || settlement.is_unrouted() {
            self.0.ack().await;
        } else if settlement.any(DeliveryOutcome::Aborted)
            || settlement.any(DeliveryOutcome::Failed)
        {
            self.0.nak().await;
        } else {
            // Dead-lettered: the sink kept the message, so the queue can let go of it.
            self.0.term().await;
        }
        Ok(())
    }

    async fn on_retry(&self, _attempt: u32, _delay: Duration) -> anyhow::Result<()> {
        self.0.in_progress().await;
        Ok(())
    }
}

#[derive(Debug)]
enum PaymentTopic {
    Settled,
    Failed,
}

#[derive(Debug, Default)]
struct Billing {
    ledger_calls: AtomicU32,
}

#[async_trait::async_trait]
impl EventConsumer for Billing {
    type Topic = PaymentTopic;

    fn subscriptions(&self) -> Subscriptions<PaymentTopic> {
        Subscriptions::new()
            .on(PaymentTopic::Settled, "payments.*.settled")
            .on(
                PaymentTopic::Failed,
                Subscription::new("payments.*.failed")
                    .with_retry_policy(RetryPolicy::exponential(3, Duration::from_millis(50)))
                    .with_concurrency(1),
            )
    }

    async fn handle_event(
        &self,
        topic: &PaymentTopic,
        delivery: &Delivery,
    ) -> Result<(), HandlerError> {
        let payment = std::str::from_utf8(delivery.message().content())?;
        let queue_id = delivery
            .transport::<QueueHandle>()
            .map_or(0, |handle| handle.0.id);
        match topic {
            PaymentTopic::Settled if payment.is_empty() => Err(HandlerError::permanent(
                anyhow::anyhow!("payment #{queue_id} has no payload"),
            )),
            PaymentTopic::Settled => {
                println!("[billing]  #{queue_id} booked {payment}");
                Ok(())
            }
            PaymentTopic::Failed => {
                // The ledger service is unavailable for the first two calls.
                if self.ledger_calls.fetch_add(1, Ordering::Relaxed) < 2 {
                    println!(
                        "[billing]  #{queue_id} ledger unavailable on attempt {}",
                        delivery.attempt()
                    );
                    return Err(anyhow::anyhow!("ledger service unavailable").into());
                }
                println!(
                    "[billing]  #{queue_id} reversed {payment} on attempt {}",
                    delivery.attempt()
                );
                Ok(())
            }
        }
    }
}

#[derive(Debug)]
struct DeadLetterLog;

#[async_trait::async_trait]
impl DeadLetterSink for DeadLetterLog {
    async fn dead_letter(&self, letter: DeadLetter) -> anyhow::Result<()> {
        println!(
            "[dlq]      {} via {} after {} attempt(s): {}",
            letter.message().topic(),
            letter.subscription().pattern(),
            letter.attempts(),
            letter.reason()
        );
        Ok(())
    }
}

#[derive(Debug)]
struct QueueBridge {
    sender: mpsc::Sender<EventMessage>,
}

impl EventEmitter for QueueBridge {
    fn get_sender(&self) -> &mpsc::Sender<EventMessage> {
        &self.sender
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let broker = EventBroker::default().with_dead_letter_sink(Arc::new(DeadLetterLog));
    broker.add_topic_consumer(Billing::default())?;

    let broker = broker.run()?;
    let bridge = QueueBridge {
        sender: broker.get_sender(),
    };

    // Messages as they arrive from the queue, each with its handle attached.
    let incoming = [
        (1, "payments.eu.settled", "P-100"),
        (2, "payments.eu.failed", "P-101"),
        (3, "payments.us.settled", ""),
        (4, "payments.us.refunded", "P-103"),
    ]
    .into_iter()
    .map(|(id, topic, payment)| {
        EventMessage::new(topic, payment)
            .map(|message| message.with_transport(QueueHandle(QueueMessage { id })))
    })
    .collect::<Result<EventSubmission, _>>()?;
    bridge.submit_event(incoming).await?;

    // Stopping waits for pending retries, so every queue message is settled afterwards.
    broker.stop().await;

    Ok(())
}
