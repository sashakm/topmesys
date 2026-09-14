use std::{
    any::Any,
    fmt::Debug,
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

use crate::{EventMessage, SubscriptionInfo};

/// Carries the transport specific part of a message received from another messaging system, like
/// a JetStream message, a Kafka offset alongside its consumer or an AMQP acker. Attach it with
/// [with_transport](EventMessage::with_transport) before submitting the message to an
/// [EventBroker](crate::EventBroker), and access it again through
/// [Delivery::transport](crate::Delivery::transport) or [EventMessage::transport].
///
/// The broker settles every message carrying a handle exactly once: after all subscriptions it was
/// routed to finished with it, or right away if no subscription matched. The [Settlement] reports
/// the [DeliveryOutcome] of every subscription, and the implementation maps it to the transport's
/// acknowledgement, e.g. an ack if everything was handled and a nak if a delivery was aborted.
///
/// Implementing the trait for a transport's own message type usually requires a newtype.
/// #### Example
/// ```
/// use topmesys::{DeliveryOutcome, EventMessage, Settlement, TransportHandle};
///
/// // Would wrap the transport's message, e.g. `async_nats::jetstream::Message`.
/// #[derive(Debug)]
/// struct JetStreamHandle {
///     stream_sequence: u64,
/// }
///
/// #[async_trait::async_trait]
/// impl TransportHandle for JetStreamHandle {
///     async fn settle(&self, settlement: &Settlement) -> anyhow::Result<()> {
///         if settlement.all_handled() || settlement.is_unrouted() {
///             // ack
///         } else if settlement.any(DeliveryOutcome::Aborted) {
///             // nak, so the message is redelivered
///         } else {
///             // term, the message was dead-lettered or failed
///         }
///         Ok(())
///     }
/// }
///
/// let message = EventMessage::new("orders.eu.created", "{}")
///     .unwrap()
///     .with_transport(JetStreamHandle { stream_sequence: 42 });
///
/// assert_eq!(message.transport::<JetStreamHandle>().unwrap().stream_sequence, 42);
/// ```
#[async_trait::async_trait]
pub trait TransportHandle: Any + Debug + Send + Sync {
    /// Called once per message, after all of its deliveries finished.
    async fn settle(&self, settlement: &Settlement) -> anyhow::Result<()>;

    /// Called before a subscription waits `delay` to make `attempt` at handling the message, e.g. to
    /// extend the message's acknowledgement deadline. Does nothing by default.
    async fn on_retry(&self, _attempt: u32, _delay: Duration) -> anyhow::Result<()> {
        Ok(())
    }
}

/// What happened to a message on one subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryOutcome {
    /// The handler succeeded, possibly after retries.
    Handled,
    /// Handling failed and a dead letter sink accepted the message.
    DeadLettered,
    /// Handling failed and no dead letter sink took the message, because none was configured or it
    /// failed itself.
    Failed,
    /// The delivery was dropped before it finished, e.g. because its subscription's worker was
    /// aborted.
    Aborted,
}

/// The outcome of one subscription a message was routed to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionOutcome {
    subscription: Arc<SubscriptionInfo>,
    outcome: DeliveryOutcome,
}

impl SubscriptionOutcome {
    pub fn subscription(&self) -> &SubscriptionInfo {
        &self.subscription
    }

    pub fn outcome(&self) -> DeliveryOutcome {
        self.outcome
    }
}

/// Reports what happened to a message on every subscription it was routed to. Handed to
/// [TransportHandle::settle] once all deliveries finished.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Settlement {
    outcomes: Vec<SubscriptionOutcome>,
}

impl Settlement {
    pub fn outcomes(&self) -> &[SubscriptionOutcome] {
        &self.outcomes
    }

    /// `true` if the message matched no subscription.
    pub fn is_unrouted(&self) -> bool {
        self.outcomes.is_empty()
    }

    /// `true` if the message was routed and every subscription handled it. An unrouted message was
    /// not handled.
    pub fn all_handled(&self) -> bool {
        !self.is_unrouted()
            && self
                .outcomes
                .iter()
                .all(|outcome| outcome.outcome == DeliveryOutcome::Handled)
    }

    /// `true` if at least one subscription ended with the given outcome.
    pub fn any(&self, outcome: DeliveryOutcome) -> bool {
        self.outcomes
            .iter()
            .any(|candidate| candidate.outcome == outcome)
    }
}

/// Collects the outcomes of a message carrying a [TransportHandle] and settles it once. The
/// message's deliveries share the settler, and the delivery finishing last settles it.
#[derive(Debug)]
pub(crate) struct Settler {
    message: Arc<EventMessage>,
    outcomes: Mutex<Vec<SubscriptionOutcome>>,
}

impl Settler {
    /// Returns `None` for messages without a transport handle, as there is nothing to settle.
    pub(crate) fn new(message: &Arc<EventMessage>, deliveries: usize) -> Option<Self> {
        message.transport_handle()?;
        Some(Self {
            message: message.clone(),
            outcomes: Mutex::new(Vec::with_capacity(deliveries)),
        })
    }

    pub(crate) fn record(&self, subscription: &Arc<SubscriptionInfo>, outcome: DeliveryOutcome) {
        self.outcomes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(SubscriptionOutcome {
                subscription: subscription.clone(),
                outcome,
            });
    }

    pub(crate) async fn settle(self) {
        let settlement = Settlement {
            outcomes: self
                .outcomes
                .into_inner()
                .unwrap_or_else(PoisonError::into_inner),
        };
        if let Some(handle) = self.message.transport_handle()
            && let Err(e) = handle.settle(&settlement).await
        {
            tracing::error!(
                "Failed to settle event on `{}`: {e:#}",
                self.message.topic()
            );
        }
    }
}
