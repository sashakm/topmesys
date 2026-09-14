use std::{fmt::Display, sync::Arc};

use crate::{EventMessage, HandlerError, SubscriptionInfo};

/// Why a message was handed to a [DeadLetterSink].
#[derive(Debug)]
pub enum DeadLetterReason {
    /// The handler failed permanently or ran out of retries.
    HandlerFailed(HandlerError),
    /// The subscription's inbox was full and its overflow policy is
    /// [Overflow::DeadLetter](crate::Overflow::DeadLetter).
    InboxFull,
}

impl Display for DeadLetterReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::HandlerFailed(error) => Display::fmt(error, f),
            Self::InboxFull => write!(f, "subscription inbox is full"),
        }
    }
}

/// A message a subscription gave up on.
#[derive(Debug)]
pub struct DeadLetter {
    pub(crate) message: Arc<EventMessage>,
    pub(crate) subscription: Arc<SubscriptionInfo>,
    pub(crate) reason: DeadLetterReason,
    pub(crate) attempts: u32,
}

impl DeadLetter {
    /// The message, including its [transport handle](EventMessage::transport).
    pub fn message(&self) -> &Arc<EventMessage> {
        &self.message
    }

    pub fn subscription(&self) -> &SubscriptionInfo {
        &self.subscription
    }

    pub fn reason(&self) -> &DeadLetterReason {
        &self.reason
    }

    /// How often the handler was called for the message, `0` if the message never reached it.
    pub fn attempts(&self) -> u32 {
        self.attempts
    }
}

/// Receives the messages subscriptions give up on: after the handler failed permanently or used up
/// all retries of its [RetryPolicy](crate::RetryPolicy), or when the subscription's inbox
/// overflowed. A sink can be set for all subscriptions with
/// [EventBroker::with_dead_letter_sink](crate::EventBroker::with_dead_letter_sink) and for a single
/// one with [Subscription::with_dead_letter_sink](crate::Subscription::with_dead_letter_sink).
/// Without a sink, these messages are logged and dropped.
///
/// For messages carrying a [TransportHandle](crate::TransportHandle), the delivery settles as
/// [DeadLettered](crate::DeliveryOutcome::DeadLettered) if the sink succeeds and as
/// [Failed](crate::DeliveryOutcome::Failed) if it returns an error.
#[async_trait::async_trait]
pub trait DeadLetterSink: std::fmt::Debug + Send + Sync {
    async fn dead_letter(&self, letter: DeadLetter) -> anyhow::Result<()>;
}
